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

## Agent instructions

- Before you add or change instructions for AI agents (`AGENTS.md`, `CLAUDE.md`, skills, rules), read ydb/agents/.agents/skills/ydb-agent-instructions/SKILL.md.
