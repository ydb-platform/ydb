# YDB Project Instructions

## Overview

This is the YDB (Yandex Database) open-source project. You are helping developers with:
1. **Code development** — C++20 (or earlier), build system, testing
2. **Documentation writing** — YDB documentation that integrates into the structure

---

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

## C++ Standards

- Use C++20 or earlier

---

## Documentation Writing Skill

**All documentation-related skills, rules, and guidelines are in `ydb/docs/.ruler/` directory.**

When helping with documentation, use the YDB Documentation Writing Skill. This skill helps writers:
1. Write new documentation articles
2. Automatically integrate them into YDB documentation structure
3. Generate glossary entries, reference materials, recipes
4. Create cross-links between related documentation
5. Generate both Russian and English versions simultaneously

The skill is described in `DOCUMENTATION_SKILL.md` - refer to it for complete 5-stage workflow.
