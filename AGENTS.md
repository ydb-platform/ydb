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
- Tests (`-tA`) run only on Linux. Do NOT attempt to run tests on macOS or Windows
- No `-j`
- No force rebuild
- Use `2>&1 | tail` for test output

## C++

- Use C++20 or earlier


