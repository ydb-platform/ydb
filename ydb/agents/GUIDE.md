# YDB Agent Guide

Root quick ref: [`AGENTS.md`](../../AGENTS.md). Prefer nearest local `AGENTS.md`.

## Layers

* `ydb/public/` — external (SDK, CLI).
* `ydb/library/` — shared internals.
* `ydb/core/` — server; not a dependency of CLI/SDK.

## Build & test

From repo root. Prefer `--build relwithdebinfo`. No `-j`, no force rebuild.
Smallest relevant folder. `2>&1 | tail` for test output.

```bash
./ya make --build relwithdebinfo <folder>
./ya make --build relwithdebinfo -tA <folder>
./ya make --build relwithdebinfo -tA <folder> -F *test-filter*
```

Details: [`BUILD.md`](../../BUILD.md) · [`TESTS.md`](TESTS.md).

## Languages & style

C++20 or earlier. [`CODESTYLE.md`](CODESTYLE.md) · [`NO_ABORT.md`](NO_ABORT.md).
Python via `ya`, packages from `contrib/python`.

## Workflow

* Smallest correct change; avoid `contrib/` / `vendor/` unless required.
* Match surrounding style; search existing code first.
* Non-trivial changes: [`CONTRIBUTING.md`](../../CONTRIBUTING.md).
* Do not commit or push unless asked.
