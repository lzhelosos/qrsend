#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]
use std::collections::HashMap;
use std::io::ErrorKind;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use eframe::egui;

use axum::body::Body;
use axum::extract::{ConnectInfo, Path as UrlPath, Query, Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{Html, IntoResponse, Json, Response};
use chrono::Datelike;
use axum::routing::{get, post, put};
use axum::Router;
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::AsyncWriteExt;
use tracing::{debug, error, info, warn};

use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

const APP_ICO: &[u8] = include_bytes!("../app.ico");

const INDEX_HTML: &str = include_str!("../static/index.html");
const PORT: u16 = 8787;
const TOKEN_HEADER: &str = "x-qrsend-token";
const MAX_NAME_CHARS: usize = 120;
const PROGRESS_LOG_EVERY: u64 = 16 * 1024 * 1024;

struct AppState {
    token: String,
    root: PathBuf,
    batches: Mutex<HashMap<String, PathBuf>>,
    /// Total bytes announced for the current batch (`POST /api/batches?total=`).
    total: AtomicU64,
    /// Bytes accepted so far for the current batch.
    received: AtomicU64,
}

type Shared = Arc<AppState>;

#[derive(Deserialize)]
struct Q {
    t: Option<String>,
    name: Option<String>,
    total: Option<u64>,
}

fn new_token() -> String {
    let bytes: [u8; 24] = rand::random();
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Returns `Some(401)` when the request lacks a valid token; logs the source and peer.
fn unauthorized(st: &AppState, peer: SocketAddr, headers: &HeaderMap, q: &Q) -> Option<Response> {
    let (given, source) = match headers.get(TOKEN_HEADER).and_then(|v| v.to_str().ok()) {
        Some(h) => (Some(h.to_string()), "header"),
        None => match q.t.clone() {
            Some(t) => (Some(t), "query"),
            None => (None, "none"),
        },
    };
    let ok = given
        .as_deref()
        .is_some_and(|g| constant_time_eq(g.as_bytes(), st.token.as_bytes()));
    if ok {
        None
    } else {
        warn!(peer = %peer, token_source = source, "unauthorized request rejected");
        Some(err(StatusCode::UNAUTHORIZED, "unauthorized"))
    }
}

fn err(code: StatusCode, key: &str) -> Response {
    (code, Json(json!({ "error": key }))).into_response()
}

fn sanitize_name(raw: &str) -> String {
    let last = raw.rsplit(['/', '\\']).next().unwrap_or("");
    let cleaned: String = last
        .chars()
        .map(|c| {
            if c.is_control() || "<>:\"|?*".contains(c) {
                '_'
            } else {
                c
            }
        })
        .take(MAX_NAME_CHARS)
        .collect();
    let trimmed = cleaned.trim().trim_matches('.').trim();
    if trimmed.is_empty() {
        "upload".to_string()
    } else {
        trimmed.to_string()
    }
}

fn split_ext(base: &str) -> (&str, &str) {
    match base.rfind('.') {
        Some(i) if i > 0 => (&base[..i], &base[i..]),
        _ => (base, ""),
    }
}

/// Creates a new file in `dir`, adding ` (n)` before the extension on name clashes.
async fn create_unique(dir: &Path, base: &str) -> std::io::Result<(tokio::fs::File, PathBuf)> {
    let (stem, ext) = split_ext(base);
    for n in 0..10_000u32 {
        let name = if n == 0 {
            base.to_string()
        } else {
            format!("{stem} ({n}){ext}")
        };
        let path = dir.join(&name);
        match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
        {
            Ok(file) => return Ok((file, path)),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    }
    Err(std::io::Error::new(ErrorKind::AlreadyExists, "too many duplicate names"))
}

/// Streams the request body into `file`, counting accepted bytes in `received`.
/// On failure this request's bytes are subtracted from `received` again.
async fn write_body(file: &mut tokio::fs::File, body: Body, received: &AtomicU64) -> Result<u64, String> {
    let mut total: u64 = 0;
    match copy_body(file, body, received, &mut total).await {
        Ok(()) => Ok(total),
        Err(e) => {
            received.fetch_sub(total, Ordering::Relaxed);
            Err(e)
        }
    }
}

async fn copy_body(
    file: &mut tokio::fs::File,
    body: Body,
    received: &AtomicU64,
    total: &mut u64,
) -> Result<(), String> {
    let mut stream = body.into_data_stream();
    let mut next_mark: u64 = PROGRESS_LOG_EVERY;
    while let Some(chunk) = stream.next().await {
        let bytes = chunk.map_err(|e| format!("body stream error after {total} bytes: {e}"))?;
        file.write_all(&bytes)
            .await
            .map_err(|e| format!("disk write error after {total} bytes: {e}"))?;
        *total += bytes.len() as u64;
        received.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        if *total >= next_mark {
            debug!(bytes_so_far = *total, "upload progress");
            next_mark += PROGRESS_LOG_EVERY;
        }
    }
    file.flush()
        .await
        .map_err(|e| format!("flush error after {total} bytes: {e}"))?;
    Ok(())
}

/// Logs every request on entry and exit: peer, method, path, status, duration.
async fn access_log(ConnectInfo(peer): ConnectInfo<SocketAddr>, req: Request, next: Next) -> Response {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let ua = req
        .headers()
        .get(header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-")
        .to_string();
    let len = req
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-")
        .to_string();
    let started = Instant::now();
    info!(peer = %peer, method = %method, path = %path, content_length = %len, ua = %ua, "request start");
    let res = next.run(req).await;
    info!(
        peer = %peer,
        method = %method,
        path = %path,
        status = res.status().as_u16(),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "request done"
    );
    res
}

async fn index(
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    State(st): State<Shared>,
    headers: HeaderMap,
    Query(q): Query<Q>,
) -> Response {
    if let Some(r) = unauthorized(&st, peer, &headers, &q) {
        return r;
    }
    info!(peer = %peer, bytes = INDEX_HTML.len(), "upload page served");
    Html(INDEX_HTML).into_response()
}

async fn favicon() -> Response {
    ([(header::CONTENT_TYPE, "image/x-icon")], APP_ICO).into_response()
}

async fn create_batch(
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    State(st): State<Shared>,
    headers: HeaderMap,
    Query(q): Query<Q>,
) -> Response {
    if let Some(r) = unauthorized(&st, peer, &headers, &q) {
        return r;
    }
    let now = chrono::Local::now();
    const MONTHS: [&str; 12] = [
        "января", "февраля", "марта", "апреля", "мая", "июня",
        "июля", "августа", "сентября", "октября", "ноября", "декабря",
    ];
    let stamp = format!(
        "{} {} {} часов {} минут",
        now.format("%-d"),
        MONTHS[now.month0() as usize],
        now.format("%-H"),
        now.format("%-M"),
    );
    let mut id = stamp.clone();
    let mut n = 2u32;
    while st.root.join(&id).exists() {
        id = format!("{stamp} ({n})");
        n += 1;
    }
    let dir = st.root.join(&id);
    if let Err(e) = tokio::fs::create_dir(&dir).await {
        error!(path = %dir.display(), error = %e, "failed to create batch folder");
        return err(StatusCode::INTERNAL_SERVER_ERROR, "create_failed");
    }
    let total = q.total.unwrap_or(0);
    st.received.store(0, Ordering::Relaxed);
    st.total.store(total, Ordering::Relaxed);
    info!(batch = %id, total_bytes = total, "batch progress reset");
    st.batches
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(id.clone(), dir.clone());
    info!(peer = %peer, batch = %id, path = %dir.display(), "batch created");
    (StatusCode::CREATED, Json(json!({ "id": id }))).into_response()
}

async fn upload(
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    State(st): State<Shared>,
    UrlPath(id): UrlPath<String>,
    headers: HeaderMap,
    Query(q): Query<Q>,
    body: Body,
) -> Response {
    if let Some(r) = unauthorized(&st, peer, &headers, &q) {
        return r;
    }
    let dir = {
        let map = st.batches.lock().unwrap_or_else(|p| p.into_inner());
        map.get(&id).cloned()
    };
    let Some(dir) = dir else {
        warn!(peer = %peer, batch = %id, "upload to unknown batch");
        return err(StatusCode::NOT_FOUND, "unknown_batch");
    };
    let raw_name = q.name.unwrap_or_default();
    let name = sanitize_name(&raw_name);
    if name != raw_name {
        info!(raw = %raw_name, sanitized = %name, "file name sanitized");
    }
    let (mut file, path) = match create_unique(&dir, &name).await {
        Ok(pair) => pair,
        Err(e) => {
            error!(batch = %id, name = %name, error = %e, "failed to create file");
            return err(StatusCode::INTERNAL_SERVER_ERROR, "create_failed");
        }
    };
    let started = Instant::now();
    info!(peer = %peer, batch = %id, file = %path.display(), "upload started");
    let bytes = match write_body(&mut file, body, &st.received).await {
        Ok(n) => n,
        Err(reason) => {
            error!(peer = %peer, batch = %id, file = %path.display(), reason = %reason, "upload failed, removing partial file");
            drop(file);
            if let Err(e) = tokio::fs::remove_file(&path).await {
                error!(file = %path.display(), error = %e, "could not remove partial file");
            }
            return err(StatusCode::BAD_REQUEST, "upload_failed");
        }
    };
    drop(file);
    let elapsed = started.elapsed().as_secs_f64();
    let mb_s = if elapsed > 0.0 {
        bytes as f64 / 1024.0 / 1024.0 / elapsed
    } else {
        0.0
    };
    let saved = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    info!(
        peer = %peer,
        batch = %id,
        saved = %saved,
        bytes = bytes,
        elapsed_s = format!("{elapsed:.2}"),
        speed_mb_s = format!("{mb_s:.2}"),
        "upload finished"
    );
    Json(json!({ "saved": saved })).into_response()
}

/// Receives client-side events from the phone page and writes them to the log.
async fn client_log(
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    State(st): State<Shared>,
    headers: HeaderMap,
    Query(q): Query<Q>,
    Json(payload): Json<Value>,
) -> Response {
    if let Some(r) = unauthorized(&st, peer, &headers, &q) {
        return r;
    }
    let event = payload.get("event").and_then(Value::as_str).unwrap_or("?").to_string();
    let data = payload.get("data").cloned().unwrap_or(Value::Null);
    let ua = payload.get("ua").and_then(Value::as_str).unwrap_or("-").to_string();
    info!(target: "phone", peer = %peer, event = %event, data = %data, ua = %ua, "phone event");
    StatusCode::NO_CONTENT.into_response()
}

fn app(state: Shared) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/favicon.ico", get(favicon))
        .route("/api/batches", post(create_batch))
        .route("/api/batches/:id/files", put(upload))
        .route("/api/log", post(client_log))
        .layer(middleware::from_fn(access_log))
        .with_state(state)
}

/// All non-loopback IPv4 addresses with their interface names.
fn lan_candidates() -> Vec<(String, Ipv4Addr)> {
    match local_ip_address::list_afinet_netifas() {
        Ok(list) => list
            .into_iter()
            .filter_map(|(name, ip)| match ip {
                IpAddr::V4(v4) if !v4.is_loopback() && !v4.is_link_local() => Some((name, v4)),
                _ => None,
            })
            .collect(),
        Err(e) => {
            error!(error = %e, "failed to list network interfaces");
            Vec::new()
        }
    }
}

/// Private LAN ranges first (192.168/16, 10/8, 172.16/12), others last.
fn lan_rank(ip: Ipv4Addr) -> u8 {
    let o = ip.octets();
    match o {
        [192, 168, _, _] => 0,
        [10, _, _, _] => 1,
        [172, b, _, _] if (16..=31).contains(&b) => 2,
        _ => 3,
    }
}

fn init_logging(logs_dir: &Path) -> Result<tracing_appender::non_blocking::WorkerGuard, Box<dyn std::error::Error>> {
    std::fs::create_dir_all(logs_dir)?;
    let stamp = chrono::Local::now().format("%Y-%m-%d_%H-%M-%S");
    let file = tracing_appender::rolling::never(logs_dir, format!("qrsend-{stamp}.log"));
    let (writer, guard) = tracing_appender::non_blocking(file);
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().with_writer(std::io::stdout))
        .with(fmt::layer().with_ansi(false).with_writer(writer))
        .init();
    Ok(guard)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cwd = std::env::current_dir()?;
    let _guard = init_logging(&cwd.join("logs"))?;
    std::panic::set_hook(Box::new(|info| {
        error!("panic: {info}");
    }));

    let root = std::env::current_exe()?
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| cwd.clone());
    info!(version = env!("CARGO_PKG_VERSION"), exe_dir = %root.display(), "qrsend starting");

    let token = new_token();
    info!("session token generated (48 hex chars, not logged)");

    let candidates = lan_candidates();
    for (name, ip) in &candidates {
        info!(interface = %name, ip = %ip, rank = lan_rank(*ip), "network interface candidate");
    }
    if candidates.is_empty() {
        warn!("no non-loopback IPv4 interfaces found");
    }

    let host = match std::env::var("QRSEND_PUBLIC_HOST") {
        Ok(v) if !v.trim().is_empty() => {
            info!(host = %v, "QRSEND_PUBLIC_HOST override used for QR URL");
            v.trim().to_string()
        }
        _ => {
            let best = candidates
                .iter()
                .min_by_key(|(_, ip)| lan_rank(*ip))
                .map(|(_, ip)| ip.to_string())
                .unwrap_or_else(|| "127.0.0.1".to_string());
            info!(host = %best, "QR URL host chosen automatically (set QRSEND_PUBLIC_HOST to override)");
            best
        }
    };
    let url = format!("http://{host}:{PORT}/?t={token}");
    info!(host = %host, port = PORT, "QR URL ready (token not logged)");

    let st: Shared = Arc::new(AppState {
        token,
        root: root.clone(),
        batches: Mutex::new(HashMap::new()),
        total: AtomicU64::new(0),
        received: AtomicU64::new(0),
    });

    let bind_error = match std::net::TcpListener::bind(("0.0.0.0", PORT)) {
        Ok(listener) => {
            let st_srv = st.clone();
            thread::spawn(move || serve(listener, st_srv));
            None
        }
        Err(e) => {
            error!(port = PORT, error = %e, "failed to bind port (is another qrsend running?)");
            Some(format!("Порт {PORT} занят ({e}). Возможно, qrsend уже запущен."))
        }
    };

    let icon = match image::load_from_memory_with_format(APP_ICO, image::ImageFormat::Ico) {
        Ok(img) => {
            let rgba = img.to_rgba8();
            let (width, height) = rgba.dimensions();
            Some(egui::IconData { rgba: rgba.into_raw(), width, height })
        }
        Err(e) => {
            error!(error = %e, "failed to decode app.ico for window icon");
            None
        }
    };
    let mut viewport = egui::ViewportBuilder::default()
        .with_title("qrsend")
        .with_inner_size([420.0, 600.0])
        .with_resizable(false)
        .with_maximize_button(false)
        .with_maximized(false);
    if let Some(icon) = icon {
        viewport = viewport.with_icon(icon);
    }
    let options = eframe::NativeOptions {
        viewport,
        ..Default::default()
    };
    eframe::run_native(
        "qrsend",
        options,
        Box::new(move |cc| Ok(Box::new(QrApp::new(cc, st, url, bind_error)))),
    )?;
    Ok(())
}

fn serve(listener: std::net::TcpListener, st: Shared) {
    let rt = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => {
            error!(error = %e, "failed to start tokio runtime");
            return;
        }
    };
    if let Err(e) = rt.block_on(run_server(listener, st)) {
        error!(error = %e, "HTTP server stopped");
    }
}

async fn run_server(listener: std::net::TcpListener, st: Shared) -> std::io::Result<()> {
    listener.set_nonblocking(true)?;
    let listener = tokio::net::TcpListener::from_std(listener)?;
    info!(addr = %listener.local_addr()?, "HTTP server listening");
    axum::serve(listener, app(st).into_make_service_with_connect_info::<SocketAddr>()).await
}

struct QrApp {
    st: Shared,
    qr: Option<egui::TextureHandle>,
    url: String,
    bind_error: Option<String>,
    copied_at: Option<f64>,
}

impl QrApp {
    fn new(cc: &eframe::CreationContext<'_>, st: Shared, url: String, bind_error: Option<String>) -> Self {
        Self {
            qr: qr_texture(cc, &url),
            st,
            url,
            bind_error,
            copied_at: None,
        }
    }
}

impl eframe::App for QrApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        egui::Panel::bottom("status").show(ui, |ui| {
            let total = self.st.total.load(Ordering::Relaxed);
            let received = self.st.received.load(Ordering::Relaxed);
            if total == 0 {
                ui.label("Загрузка: ожидание");
            } else {
                let frac = (received as f32 / total as f32).min(1.0);
                ui.add(
                    egui::ProgressBar::new(frac)
                        .show_percentage()
                        .text(format!("{} из {} МБ", mib(received), mib(total))),
                );
            }
        });
        egui::CentralPanel::default().show(ui, |ui| {

            if let Some(err) = &self.bind_error {
                ui.colored_label(egui::Color32::RED, err);
            }
            ui.add_space(8.0);
            // Fit the starting window width; on large (maximized) windows use 1/4 of the width.
            let avail = ui.available_width();
            let side = if avail > 900.0 { avail / 4.0 } else { avail - 16.0 };
            let (rect, _) = ui.allocate_exact_size(egui::vec2(side, side), egui::Sense::hover());
            let painter = ui.painter_at(rect);
            painter.rect_filled(rect, 8.0, egui::Color32::WHITE);
            paint_aurora(&painter, rect, ui.ctx().input(|i| i.time) as f32);
            if let Some(tex) = &self.qr {
                painter.image(
                    tex.id(),
                    rect,
                    egui::Rect::from_min_max(egui::pos2(0.0, 0.0), egui::pos2(1.0, 1.0)),
                    egui::Color32::WHITE,
                );
            }
            ui.add_space(8.0);
            ui.label("Отсканируйте QR телефоном");
            if ui.link(&self.url).clicked() {
                ui.ctx().copy_text(self.url.clone());
                self.copied_at = Some(ui.ctx().input(|i| i.time));
            }
            let now = ui.ctx().input(|i| i.time);
            match self.copied_at {
                Some(t) if now - t < 2.0 => {
                    ui.colored_label(egui::Color32::from_rgb(0, 160, 80), "Ссылка скопирована");
                    ui.ctx().request_repaint_after(Duration::from_millis(100));
                }
                _ => {}
            }
            ui.add_space(12.0);
            ui.label("Папка загрузок:");
            ui.horizontal(|ui| {
                ui.monospace(self.st.root.display().to_string());
                if ui.button("Открыть").clicked() {
                    if let Err(err) = std::process::Command::new("explorer").arg(&self.st.root).spawn() {
                        error!("не удалось открыть папку загрузок: {err}");
                    }
                }
            });
        });
        ui.ctx().request_repaint_after(Duration::from_millis(33));
    }
}

fn mib(bytes: u64) -> String {
    format!("{:.1}", bytes as f64 / 1_048_576.0)
}

fn paint_aurora(painter: &egui::Painter, rect: egui::Rect, t: f32) {
    // Soft violet -> emerald bands that drift over time, fading out downward.
    const COLS: usize = 48;
    const VIOLET: [f32; 3] = [140.0, 70.0, 220.0];
    const EMERALD: [f32; 3] = [40.0, 210.0, 150.0];
    let mut mesh = egui::Mesh::default();
    for i in 0..=COLS {
        let fx = i as f32 / COLS as f32;
        let x = rect.left() + rect.width() * fx;
        let phase = fx * 6.0 + t * 0.6;
        let mix = 0.5 + 0.5 * (phase * 0.7 + t * 0.25).sin();
        let lerp = |a: f32, b: f32| a + (b - a) * mix;
        let color = [
            lerp(VIOLET[0], EMERALD[0]),
            lerp(VIOLET[1], EMERALD[1]),
            lerp(VIOLET[2], EMERALD[2]),
        ];
        let strength = 0.10 + 0.06 * (phase * 0.5).sin();
        let top_y = rect.top() + rect.height() * (0.10 + 0.08 * (phase * 0.4 + 1.3).sin());
        let bottom_y = rect.top() + rect.height() * 0.6;
        let top = egui::Color32::from_rgba_unmultiplied(
            color[0] as u8,
            color[1] as u8,
            color[2] as u8,
            (255.0 * strength) as u8,
        );
        let bottom = egui::Color32::from_rgba_unmultiplied(color[0] as u8, color[1] as u8, color[2] as u8, 0);
        mesh.colored_vertex(egui::pos2(x, top_y), top);
        mesh.colored_vertex(egui::pos2(x, bottom_y), bottom);
        if i > 0 {
            let base = (2 * (i - 1)) as u32;
            mesh.add_triangle(base, base + 1, base + 2);
            mesh.add_triangle(base + 1, base + 3, base + 2);
        }
    }
    painter.add(egui::Shape::mesh(mesh));
}

fn qr_texture(cc: &eframe::CreationContext<'_>, url: &str) -> Option<egui::TextureHandle> {
    let code = match qrcode::QrCode::new(url.as_bytes()) {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "QR encode failed");
            return None;
        }
    };
    // Light modules and quiet zone are transparent so the glow shows through; dark modules stay opaque.
    const QUIET: usize = 4;
    let w = code.width();
    let size = w + 2 * QUIET;
    let mut px = vec![egui::Color32::TRANSPARENT; size * size];
    for (i, c) in code.to_colors().into_iter().enumerate() {
        if c == qrcode::Color::Dark {
            let (x, y) = (i % w + QUIET, i / w + QUIET);
            px[y * size + x] = egui::Color32::BLACK;
        }
    }
    let img = egui::ColorImage::new([size, size], px);
    Some(cc.egui_ctx.load_texture("qr", img, egui::TextureOptions::NEAREST))
}
