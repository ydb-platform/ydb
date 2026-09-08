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

## Documentation Skills

**All documentation-related skills, rules, and guidelines are in the
`ydb/docs/.ruler/` directory.** Run `ruler apply --skills` from
`ydb/docs` to distribute the skills to supported AI agents.

### Skill Routing

Use the matching skill for the task:

- `ydb-documentation`: document a feature or functionality and integrate it
  into the documentation structure.
- `generate-internal-changelog`: prepare internal YDB Server feature release
  notes. Invoke it for requests such as "сделай внутренний релиз",
  "подготовь внутренний релиз", or "internal release".

### Documentation Inputs

Before using `ydb-documentation`, gather the feature facts: description,
parameters, examples, limits, context, and implementation source.

Before using `generate-internal-changelog`, provide the three-component
release identifier when known, for example `26.3.1`. The skill derives the
public release line, target stable branch, previous stable branch, and Tracker
candidate set.

See `skills/ydb-documentation/SKILL.md` and
`skills/generate-internal-changelog/SKILL.md` for the complete workflows.
