# YDB

Quick reference for AI agents. Run `./ya` from the **repository root**.

Before changing code, read [`ydb/agents/AGENTS.md`](ydb/agents/AGENTS.md).
Style: [`ydb/agents/CODESTYLE.md`](ydb/agents/CODESTYLE.md).
Tests: [`ydb/agents/TESTS.md`](ydb/agents/TESTS.md).

## Build & Test

`<folder>` is a path relative to the repository root (e.g. `ydb/core/kqp`).
`-F *test-filter*` is a glob matching test names.

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
- No force rebuild (`-r`, `-R`, or similar flags)
- Use `2>&1 | tail` for test output

Use C++20 or earlier. See [`ydb/agents/AGENTS.md`](ydb/agents/AGENTS.md) for everything else.
