# YDB Agent Guide

Detailed instructions for AI agents working in the YDB monorepo.
For the quick reference, see the root [`AGENTS.md`](../AGENTS.md).

## Architecture boundaries

Respect layer boundaries when adding dependencies (`PEERDIR` in `ya.make`):

- `ydb/public/` — code that can be used by external consumers (SDK, CLI).
- `ydb/library/` — internal shared code for YDB components.
- `ydb/core/` — server internals; must not be depended on by CLI or public SDK.

## Nested agent instructions

Some subdirectories have their own `AGENTS.md` with area-specific rules.
Read the nearest `AGENTS.md` when working in that tree:

| Path | Topics |
|------|--------|
| [`ydb/public/lib/ydb_cli/AGENTS.md`](../ydb/public/lib/ydb_cli/AGENTS.md) | CLI architecture, dependency rules, changelog |
| [`ydb/tools/mnc/AGENTS.md`](../ydb/tools/mnc/AGENTS.md) | Python MNC tool: error handling, output, testing |

If you add substantial area-specific rules, consider creating a local
`AGENTS.md` rather than growing this file.

## Build & Test

YDB uses `./ya` (Yatool) from the repository root. See also [`BUILD.md`](../BUILD.md)
and [Yatool documentation](https://ydb.tech/docs/en/development/build-ya).

### Common commands

```bash
# Build a target
./ya make --build relwithdebinfo <folder>

# Build YDB Server
./ya make ydb/apps/ydbd --build relwithdebinfo

# Build YDB CLI
./ya make ydb/apps/ydb --build relwithdebinfo

# Run all tests in a folder (tests include build)
./ya make --build relwithdebinfo -tA <folder>

# Run a specific test
./ya make --build relwithdebinfo -tA <folder> -F *test-filter*

# Run a specific test with retries
./ya make --build relwithdebinfo -tA <folder> -F *test-filter* --test-retries N
```

### Build rules for agents

- Prefer `--build relwithdebinfo` (faster builds, uses remote cache).
- Do **not** pass `-j` (parallelism is managed by ya).
- Do **not** force rebuild.
- Use `2>&1 | tail` to capture test output.
- Build and test the smallest relevant folder, not the entire repository.

### Flag reference

| Flag | Meaning |
|------|---------|
| `-tA` | Run tests recursively in the given folder |
| `-F *pattern*` | Filter tests by name (glob) |
| `--build relwithdebinfo` | Release build with debug info (recommended) |

### Test layout

See [`TESTS.md`](TESTS.md) for C++ test framework conventions.

- **C++ unit tests** live in `ut/` subdirectories next to the code they test.
  `ya.make` uses `UNITTEST_FOR(...)` and lists sources in `TEST_SRCS()`.
- **Python tests** use `unittest`; test files are listed in `TEST_SRCS()` in
  the module's `ya.make`.
- **Functional tests** are under `ydb/tests/functional/`.
- **Compatibility tests** are under `ydb/tests/compatibility/` — place tests
  in the folder matching the feature area.

CLI tests (`ya make -tA`) can be run only on Linux.

## Languages

Coding style rules are in [`CODESTYLE.md`](CODESTYLE.md).

### C++

- Use C++20 or earlier.
- See [`TESTS.md`](TESTS.md) for unit test conventions.

### Python

- Python code is built through `ya`.
- Third-party packages come from `contrib/python`.
- Use `unittest` for tests; for async code use `unittest.IsolatedAsyncioTestCase`.
- See [`ydb/tools/mnc/AGENTS.md`](../ydb/tools/mnc/AGENTS.md) for detailed
  Python CLI conventions.


## Agent workflow

### Scope

- Make the smallest correct change. Do not refactor or clean up unrelated code.
- Do not edit `contrib/` or `vendor/` unless the task explicitly requires it.
- Match the style and abstractions of the surrounding code (see [`CODESTYLE.md`](CODESTYLE.md)).

### Before coding

- Search for existing implementations before writing new code.
- Check for a local `AGENTS.md` in the working directory.
- For new features or non-trivial bug fixes, check if there is a GitHub issue
  (see [`CONTRIBUTING.md`](../CONTRIBUTING.md)).

### Testing

- Run tests for the folder you changed: `./ya make --build relwithdebinfo -tA <folder>`.
- If you fix a specific test, filter with `-F`.
- Do not add tests that only assert trivially true behavior.

### Git

- Do not create commits unless explicitly asked.
- Do not push unless explicitly asked.
- Do not amend commits that have been pushed.

## Further reading

- [`CODESTYLE.md`](CODESTYLE.md) — coding style guidelines
- [`TESTS.md`](TESTS.md) — test conventions
- [`BUILD.md`](../BUILD.md) — building YDB Server and CLI from source
- [`CONTRIBUTING.md`](../CONTRIBUTING.md) — contribution process
- [`README.md`](../README.md) — project overview
- [YDB documentation](https://ydb.tech/docs/en/) — user and developer docs
