# Bun

Платформенные ресурсы Bun для Linux и macOS (x86_64 и ARM64).
Каждый ресурс содержит один исполняемый файл `bun`.

Бинарники скачиваются из [официальных релизов Bun](https://github.com/oven-sh/bun/releases)
и загружаются в Sandbox скриптом [upload.sh](upload.sh).
Идентификаторы текущей стандартной версии находятся в [bun.sbr.json](bun.sbr.json),
закреплённых версий — в `<версия>/bun.sbr.json`.

Ресурсы используют:

- `ya tool bun` — через `formula` в `build/tools/tools/bun.tool.json`;
- сборки с `TS_USE_BUN()` — через корневой [ya.make](ya.make);
- `nots/cli` — через `ya tool bun`.
