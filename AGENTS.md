# Project

## Build & Test

```bash
# Build
./ya make --build relwithdebinfo <folder>

# Run all tests
./ya make --build relwithdebinfo -tA <folder>

# Run specific test
./ya make --build relwithdebinfo -tA <folder> -F *test-filter*
```

- Tests include build
- No `-j`
- No force rebuild
- Use `2>&1 | tail` for test output

## Documentation (ydb/docs)

- SDK docs use `{% list tabs %}` for languages at the top level
- Frameworks/connectors within a language go as nested `{% list tabs %}` inside the language tab (e.g. Go → `Native SDK` | `database/sql`, Python → `Native SDK` | `Native SDK (Asyncio)`)
- Sub-variants within a framework use `{% cut "..." %}` (e.g. inside `database/sql`: `{% cut "С помощью коннектора" %}`, `{% cut "С помощью строки подключения" %}`)
- If a feature is not supported by an SDK, add the language tab and state it explicitly

## C++

- Use C++20 or earlier


