# YDB CLI

## Architecture

- App main entrypoint: `ydb/apps/ydb/` (main.cpp, version, update commands)
- Main CLI library: `ydb/public/lib/ydb_cli/` (commands, common, dump, import, topic)
- This library is reused by other internal CLI targets

## Build

Build the CLI in release: `./ya make -r ydb/apps/ydb` (`-r` = `--build release`).
Prefer `-r` over the repo-wide `relwithdebinfo` default; that default mainly helps the
server (crash dumps with symbols), not the CLI.

## Dependency rules

CLI code MUST NOT depend on `ydb/core/` or its subdirectories.
This breaks Windows builds and violates architecture boundaries.
If functionality is needed in both CLI and server:
- `ydb/public/lib/` for public reusable libraries
- `ydb/library/` for internal shared code

## Command and option descriptions

- Write clear descriptions for commands and options. Keep them as concise as possible without omitting information needed to use them correctly.
- When an option accepts a predefined set of values, list the supported values in its description.
- Add those values to the option's autocompletion suggestions with `ChoicesWithCompletion`. Keep the description, completion suggestions, and accepted values in sync.

## Changelog

User-visible changes (new/changed commands, options, significant behavior changes) MUST be recorded in `ydb/apps/ydb/CHANGELOG.md`.
Hidden commands/options do NOT need a changelog entry.
Follow the existing bullet-point style in the file.

## Tests

Run with `./ya make -tA <dir>` (builds the binary first; Linux only).

- Unit tests (C++): `ut/` dirs next to the code (e.g. `ydb/public/lib/ydb_cli/common/.../ut/`).
- Binary/integration tests (C++): `ydb/apps/ydb/ut/` — drive the built `ydb` against a YDB recipe.
- Functional tests (Python): `ydb/tests/functional/ydb_cli/` — start a real cluster and run `ydb`; interactive cases use pexpect, golden output in `canondata/`.
