# Where an instruction goes

## Decision table

| The rule is about | Put it in | Form |
|---|---|---|
| One directory, one or two sentences | `<dir>/AGENTS.md` | a line |
| One directory, a task with steps | `<dir>/.agents/skills/<name>/SKILL.md` | a skill, plus one pointer line in `<dir>/AGENTS.md` |
| Several directories changed together by one team | the closest common parent, same forms as above | |
| A directory that already has `RULES.md` or `rules/*.md` (plain files reached by a link from `AGENTS.md`; no tool loads them on its own) | that existing file | keep its form; do not start a second one |
| The whole repo (build, tests, style, safety) | `ydb/agents/<FILE>.md` | a doc, linked from `ydb/agents/GUIDE.md` |
| Every session of every developer | root `AGENTS.md` | one link line, with a written reason |

`<dir>` is the directory that holds the code the rule is about, for example `ydb/core/blobstorage/pdisk`.

## Why the nearest place

A rule in a file that loads at start is paid for in every session, also when the task has nothing to do with it. In `ydb/core/blobstorage/pdisk/AGENTS.md` the same rule is paid for only when an agent works on PDisk files.

## Root placement needs a reason

Answer these questions in the pull request:

1. Which tasks need this rule in every session, in every directory?
2. Why is a link from the root file to a nested file not enough?
3. How many lines does it add to the root file?

Example, the reason for the root line that points to this skill: the task "change agent instructions" can start in any directory, so no nested file is loaded for it. Tools find this skill on their own only when the session starts in or below `ydb/agents/` or edits files there. The root line is the only path that works everywhere. It adds a heading and one bullet.

## Budgets

| File | Budget | Source |
|---|---|---|
| `AGENTS.md` | 50 lines | this repo; loaded before the agent reads anything else |
| `SKILL.md` | 200 lines, hard limit 500 | this repo; [Claude Code](https://code.claude.com/docs/en/skills) recommends staying under 500 lines |
| `description` | 1024 characters | [Agent Skills specification](https://agentskills.io/specification) |
| All `AGENTS.md` from root to `<dir>` | 32 KiB | [Codex](https://learn.chatgpt.com/docs/agent-configuration/agents-md) stops reading after this size; Claude Code reads the same text through `CLAUDE.md` and only skips a file above 4 MiB ([memory](https://code.claude.com/docs/en/memory)) |
| Frontmatter keys | `name`, `description` | the only keys every tool understands, see `tool-compatibility.md` |

`check.py` reports these; line budgets and frontmatter keys as warnings, the rest as errors. It also requires skill names to start with `ydb-`, rejects a name already used in the repo, and warns about names that look alike.

## Layout of one skill level

```text
<dir>/
  AGENTS.md                      loaded by Codex when the session starts here or below, by OpenCode when it is the nearest one, by Cursor when files here are edited, by Claude Code through CLAUDE.md
  CLAUDE.md                      one line: @./AGENTS.md; Claude Code reads only CLAUDE.md
  .claude -> .agents             symlink; Claude Code reads skills only from .claude/skills
  .agents/skills/<name>/
    SKILL.md                     frontmatter name + description, then short numbered text
    references/*.md              long tables and background, read on demand
    scripts/<name>.py            Python 3.8, standard library only
    scripts/tests/test_<name>.py unittest for each script
```

## Examples to copy

- A router, that is an `AGENTS.md` which only points to other files, with a table: `ydb/core/blobstorage/AGENTS.md`.
- Short router plus skill: `ydb/core/blobstorage/pdisk/AGENTS.md` and `ydb/core/blobstorage/pdisk/.agents/skills/ydb-pdisk-development/SKILL.md`.
- Nested `CLAUDE.md` include: `ydb/core/nbs/CLAUDE.md`.
