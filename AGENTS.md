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
- No `-j`
- No force rebuild
- Use `2>&1 | tail` for test output

## C++

- Use C++20 or earlier

## Documentation Skills & Guidelines

### Documentation Writer Skill

See [DOCUMENTATION_SKILL.md](./DOCUMENTATION_SKILL.md) for the complete documentation writing skill. This skill helps writers create new documentation that properly integrates into YDB's documentation structure, including:

- Automatic placement of articles in the correct sections (based on DOCUMENTATION_MODEL.yaml)
- Generation of glossary entries, reference materials, and examples
- Creation of cross-links between related documentation
- Simultaneous generation of Russian and English versions
- Updates to table of contents, documentation model, and redirects

The skill is designed to work across all AI agents (Claude Code, Cursor, and others).

### Documentation Formatting Rules

See `ydb/docs/.ruler/FORMAT_RULES.md` for complete rules.

**Markdown linting (before commit):**
- **MD032**: Empty lines before/after lists
- **MD051**: Link anchors must exist (explicit `{#anchor}` or auto-generated from headings)
- **MD009**: No trailing whitespace

**Link validation:**
- Relative paths must exist (resolved relative to current file)
- Anchors in `[text](file.md#anchor)` must be present in target file
- Internal links `[text](#anchor)` must have anchor in same file

**Code blocks:**
- SQL: use `yql` dialect (` ```yql `)


