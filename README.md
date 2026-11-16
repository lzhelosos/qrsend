# QRSend
QRSend - a utility for transferring files directly to a Windows PC over a local network. The desktop application, built with Rust (egui/eframe), runs an HTTP server on port 8787 and displays a QR code containing the URL and access token. Users simply scan the QR code with their phone, open the webpage, select files, and upload them directly to a folder on the PC. No app installation is required on the phone.

QRSend - утилита для прямой передачи файлов на Windows по локальной сети. Desktop-приложение на Rust (egui/eframe) поднимает HTTP-сервер на порту 8787, показывает QR с URL и токеном; телефон открывает страницу, выбирает файлы и загружает их в папку на ПК. Установка приложения на телефон не требуется. 

- [x] Отправка с Android на Windows
- [x] Отправка с iOS на Windows
- [x] Отправка с Windows на Windows

## Требования для сборки
Windows 10/11
Rust (Cargo)

## Сборка
```bash
cargo build --release
```








