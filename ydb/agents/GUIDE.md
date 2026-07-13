# YDB Agent Guide

Detailed instructions for AI agents working in the YDB monorepo.
Quick reference: root [`AGENTS.md`](../../AGENTS.md).

## Architecture boundaries

Respect layer boundaries when adding dependencies (`PEERDIR` in `ya.make`):

- `ydb/public/` — code for external consumers (SDK, CLI).
- `ydb/library/` — internal shared YDB libraries.
- `ydb/core/` — server internals; must not be depended on by CLI or public SDK.

## Nested agent instructions

Read the nearest `AGENTS.md` when working in that tree.
Add area-specific rules in a local `AGENTS.md` rather than growing this file.

## Build & Test

Run `./ya` from the repository root. See [`BUILD.md`](../../BUILD.md) and
[Yatool docs](https://ydb.tech/docs/en/development/build-ya).

```bash
./ya make --build relwithdebinfo <folder>
./ya make ydb/apps/ydbd --build relwithdebinfo
./ya make ydb/apps/ydb --build relwithdebinfo
./ya make --build relwithdebinfo -tA <folder>
./ya make --build relwithdebinfo -tA <folder> -F *test-filter*
./ya make --build relwithdebinfo -tA <folder> -F *test-filter* --test-retries N
```

- Prefer `--build relwithdebinfo`. No `-j`. No force rebuild (`-r`, `-R`, …).
- Use `2>&1 | tail` for test output.
- Build and test the smallest relevant folder.

Test types and frameworks: [`TESTS.md`](TESTS.md).

## Languages

C++20 or earlier. Style: [`CODESTYLE.md`](CODESTYLE.md). Tests: [`TESTS.md`](TESTS.md).
Python via `ya`, packages from `contrib/python`.

## Agent workflow

- Make the smallest correct change; do not edit `contrib/` or `vendor/` unless required.
- Match surrounding code style ([`CODESTYLE.md`](CODESTYLE.md)).
- Search for existing code; check for a local `AGENTS.md`.
- For non-trivial changes, check [`CONTRIBUTING.md`](../../CONTRIBUTING.md).
- Run `./ya make --build relwithdebinfo -tA <folder>` for the area you changed.
- Do not create commits or push unless explicitly asked.
