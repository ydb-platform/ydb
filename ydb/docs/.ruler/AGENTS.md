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

### When to Use the Documentation Skill

When a developer needs to document a feature or functionality:
1. **First**: Gather all information (фактура) - description, parameters, examples, limits, context
2. **Then**: Use the Documentation Writing Skill to integrate it into the structure

### What the Skill Does

The skill helps writers:
1. Analyze the provided information and documentation structure
2. Create comprehensive plan (main article, glossary, reference, recipes, cross-links, TOC, redirects)
3. Generate documented articles integrated into YDB documentation
4. Create both Russian and English versions simultaneously (not translation)

### Important: Provide Complete Information

The AI cannot invent details. You must provide:
- **Description** - What does this do?
- **Parameters/Options** - What can be configured?
- **Examples** - Code samples, command examples, configuration
- **Limitations** - What are the constraints?
- **Context** - How does it relate to other features?
- **Source** - PR, Issue, or specification reference

The more information you provide, the better the documentation.

See `DOCUMENTATION_SKILL.md` for complete 5-stage workflow details.
