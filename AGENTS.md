## Project

## Structure

Every directory is a library or program.

- `/ydb/core`: Core modules
- `/ydb/library`: YDB libraries
- `/ydb/docs`: YDB documentation
- `/library`, `/util`: Common libraries; never change them

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
- No `-j`, no force rebuild
- Use `2>&1 | tail` for test output

## C++

- Use C++20 or earlier


