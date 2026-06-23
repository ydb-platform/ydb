# YDB

## Build & Test

```bash
# Build
./ya make --build relwithdebinfo <folder>

# Run all tests (includes build)
./ya make --build relwithdebinfo -tA <folder>

# Run specific test filter
./ya make --build relwithdebinfo -tA <folder> -F *test-filter*

# Run specific test sizes (small|medium|large)
./ya make --build relwithdebinfo -tA <folder> --test-size=small
```

- Tests include build
- No `-j` flag (ya handles parallelism internally)
- No force rebuild flag
- Use `2>&1 | tail` for test output

### Build Presets

| Preset | Use Case |
|--------|----------|
| `relwithdebinfo` | Default, recommended (uses remote cache) |
| `release` | Release build |
| `release-asan` | AddressSanitizer |
| `release-tsan` | ThreadSanitizer |
| `release-msan` | MemorySanitizer |

### Important Build Targets

```bash
# YDB server
./ya make ydb/apps/ydbd --build relwithdebinfo

# YDB CLI
./ya make ydb/apps/ydb --build relwithdebinfo
```

## Architecture

- `ydb/` - Main YDB server code
  - `ydb/apps/` - Executables (ydbd, ydb CLI)
  - `ydb/core/` - Core database engine (internal)
  - `ydb/public/lib/` - Public APIs and reusable libraries
  - `ydb/tests/` - Integration tests
- `yql/` - YQL (YDB Query Language)
- `util/` - Utility libraries
- `library/` - Additional libraries
- `contrib/` - Contributed/vendored code

## C++

- Standard: C++20 or earlier

## Documentation

Documentation is located in `ydb/docs/en/` (English) and `ydb/docs/ru/` (Russian).
When exploring code, also check documentation for insights.

## CLI

See `ydb/public/lib/ydb_cli/AGENTS.md` for CLI modification instructions
