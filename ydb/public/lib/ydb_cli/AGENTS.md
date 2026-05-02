# YDB CLI

## Architecture

- App main entrypoint: `ydb/apps/ydb/` (main.cpp, version, update commands)
- Main CLI library: `ydb/public/lib/ydb_cli/` (commands, common, dump, import, topic)
- This library is reused by other internal CLI targets

## Dependency rules

CLI code MUST NOT depend on `ydb/core/` or its subdirectories.
This breaks Windows builds and violates architecture boundaries.
If functionality is needed in both CLI and server:
- `ydb/public/lib/` for public reusable libraries
- `ydb/library/` for internal shared code

## Changelog

User-visible changes (new/changed commands, options, significant behavior changes) MUST be recorded in `ydb/apps/ydb/CHANGELOG.md`.
Hidden commands/options do NOT need a changelog entry.
Follow the existing bullet-point style in the file.
