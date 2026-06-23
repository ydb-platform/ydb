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
- Use `./ya make --help` if you need info about flags and build presets

### Important Build Targets

```bash
# YDB server
./ya make ydb/apps/ydbd --build relwithdebinfo

# YDB CLI
./ya make ydb/apps/ydb --build relwithdebinfo
```

## C++

- Standard: C++20 or earlier

## Documentation

Documentation is located in `ydb/docs/en/` (English) and `ydb/docs/ru/` (Russian).
When exploring code, also check documentation for insights.

## Important

- Add `AFL_ENSURE` for all your assumptions about code that can be checked in `O(1)`
- Do not add silent fallbacks. If anything goes wrong don't ignore it and return error.
