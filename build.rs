fn main() {
    println!("cargo:rerun-if-changed=app.ico");
    if std::env::var_os("CARGO_CFG_WINDOWS").is_some() {
        let mut res = winres::WindowsResource::new();
        res.set_icon("app.ico");
        res.compile().expect("failed to embed app.ico into qrsend.exe");
    }
}
