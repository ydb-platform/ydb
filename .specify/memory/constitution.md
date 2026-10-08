<!-- Sync Impact Report: template -> 1.0.0. Added confirmed build/test and C++ rules.
No deferred placeholders. Source: AGENTS.md and user instructions for this worktree. -->
# YDB Constitution

## Core Principles

### I. Project build and test tooling

Build and test with ordinary `./ya make --build relwithdebinfo`.
Tests include compilation. Use `-tA` for tests and `-F` for a focused filter.
Use `~/arcadia/ya` for operations other than building or testing that require ya.

### II. Build resource discipline

Do not pass `-j` or force a rebuild. Pipe test output through `2>&1 | tail`.
Use `--test-retries N` when repeated execution is needed.

### III. C++ compatibility

Use C++20 or earlier.

## Project Instructions

Apply the root AGENTS.md and applicable nested AGENTS.md files to touched code.
This constitution records confirmed project rules; it does not add repository-wide standards.

## Development Workflow

For this requested Spec Kit workflow, keep requirements, plan, tasks and actual verification
results consistent. Do not represent unexecuted or failed checks as successful.

## Governance

Update this document when confirmed project instructions change. Record the reason and version
change; use major versions for incompatible rule changes, minor for additions, patch for wording.
Review task plans against these rules. Explicit user instructions take precedence.

**Version**: 1.0.0 | **Ratified**: 2026-10-05 | **Last Amended**: 2026-10-05
