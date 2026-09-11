# Project

## Build & Test

```bash
# Build
./ya make --build relwithdebinfo <folder>

# Run all tests
./ya make --build relwithdebinfo -tA <folder>

# Run specific test
./ya make --build relwithdebinfo -tA <folder> -F *test-filter*

# Run tests repeatedly (e.g. to catch flakes)
./ya make --build relwithdebinfo -tA <folder> -F *test-filter* --test-retries N
```

- Tests include build
- No `-j`
- No force rebuild
- Use `2>&1 | tail` for test output

Test filters should include part of full test name. Full test name format depends on test framework:

- For C++ unit test: `<suite name>::<test name>`
- For python pytest: `<file>.<class>.<test name>[<fixture params>]`

## C++

- Use C++20 or earlier

## Shared rules

- ydb/agents/GUIDE.md: repo layers, build and test commands, workflow. Read before any code change.
- ydb/agents/TESTS.md: how tests are laid out and declared. Read before adding tests.
- ydb/agents/CODESTYLE.md: C++ coding style. Read before writing C++.
- ydb/agents/NO_ABORT.md: never abort the process; what to use instead of `Y_ABORT`. Read before writing C++.
- ydb/agents/BACKWARD_COMPATIBILITY.md: proto fields and storage formats. Read before changing them.
- ydb/agents/developer-ui-security-guidelines.md: security rules for monitoring pages. Read only when changing the developer UI.
