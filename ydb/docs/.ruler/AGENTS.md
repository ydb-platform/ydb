# YDB Documentation Agent Instructions

## Shared policy

Before writing or reviewing YDB documentation, read
`ydb/docs/.ruler/DOCUMENTATION_POLICY.md` and every normative file it lists.
The policy is the only source of shared writing rules.

## Skill routing

Use the matching skill from `ydb/docs/.ruler/skills/`:

- `ydb-documentation` for product documentation, glossary, reference, recipes,
  TOC, and redirects.
- `generate-internal-changelog` for internal YDB Server feature release notes.
- `ydb-server-release-notes` for public YDB Server release notes and backports.
- `ydb-release-publication` for publication after the release-notes gate.

Read the selected `SKILL.md` completely before starting its workflow.

## Distribution

Run `ruler apply --skills` from `ydb/docs` to distribute rules and skills to
the supported AI agents.
