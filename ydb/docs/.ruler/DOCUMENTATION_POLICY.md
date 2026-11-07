# Canonical YDB documentation policy

This file is the single entry point for YDB documentation rules. Skills must read this file first and then read every normative file listed below. Do not copy or summarize these rules in a skill.

## Normative files

Read these files completely, in this order:

1. `GENERAL_RULES.md` defines the audience and baseline language quality.
2. `FORMAT_RULES.md` defines Markdown, YFM, code-block, link, and anchor requirements.
3. `DOCUMENTATION_RULES.md` defines content-quality checks and their priorities.

All paths are relative to this file in `ydb/docs/.ruler/`.

## Precedence

1. Explicit user instructions take precedence.
2. Within the normative files, a later file in the reading order takes precedence. In particular, `DOCUMENTATION_RULES.md` wins if another file conflicts with it.
3. The normative files above define the shared writing and review policy.
4. A workflow skill may add task-specific steps, output formats, or review heuristics, but must not redefine or duplicate the shared policy.

If a workflow instruction conflicts with a normative file, follow the normative file and report the conflict.

## Maintenance

- Change documentation rules only in the normative files above.
- Update this file only when the normative file set or its reading order changes.
- Do not add copied rule summaries to `ydb-documentation`, `pr-review`, or another workflow skill.
