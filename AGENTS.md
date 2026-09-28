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
- Do not edit sources during compilation: this can cause `null character ignored` errors.
- Use `2>&1 | tail` for test output

- Test name formats for `-F`: ydb/agents/TESTS.md

## C++

- Use C++20 or earlier

## Attribution

- Do not name branches or repository paths after AI coding assistants.
- Do not mention AI coding assistants as authors, tools, or promotional credits in commit messages, including `Co-authored-by` trailers.
