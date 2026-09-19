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
- `ydb-server-release-notes`: prepare or audit public YDB Server release notes,
  including RC notes and stable backports.
- `ydb-release-publication`: publish YDB only after the release-notes gate,
  including stable documentation, GitHub Release, default branch, and Club YDB.

### Documentation Inputs

Before using `ydb-documentation`, gather the feature facts: description,
parameters, examples, limits, context, and implementation source.

Before using `generate-internal-changelog`, provide the exact four-component RC
tag when known, for example `26.3.1.16`. The skill derives the public RC line,
verifies the tag in its patch branch, and builds the Tracker candidate set.

Before using `ydb-server-release-notes`, provide the target tag or release line
and whether the release is an RC or final. Before using
`ydb-release-publication`, provide the merged release-notes PR and exact server
version.

See the matching directory under `skills/` for the complete workflow.
