# How the four tools find instructions

Every row comes from the linked documentation. When you change this file, read the documentation again.

| Tool | Reads skills from | Reads rules from | Nested directories | Source |
|---|---|---|---|---|
| Claude Code | `.claude/skills/<name>/SKILL.md`; `.agents/skills` is not a documented location | `CLAUDE.md` and `.claude/rules/*.md`; the line `@./AGENTS.md` includes `AGENTS.md` | nested `.claude/skills` and `CLAUDE.md` load when the agent works on files in that directory | https://code.claude.com/docs/en/skills, https://code.claude.com/docs/en/memory |
| Codex | `.agents/skills` in every directory from the current directory up to the repo root | `AGENTS.md` from the repo root down to the current directory, joined; stops after 32 KiB | `AGENTS.md` below the current directory is not loaded | https://learn.chatgpt.com/docs/build-skills, https://learn.chatgpt.com/docs/agent-configuration/agents-md |
| Cursor | `.agents/skills`, `.cursor/skills`, `.claude/skills`, `.codex/skills`, in any subdirectory | `AGENTS.md` in the root and in subdirectories; nested files add to the parent ones | a nested skill is shown when the agent works under that directory | https://cursor.com/docs/skills, https://cursor.com/docs/context/rules |
| OpenCode | `.opencode/skills`, `.claude/skills`, `.agents/skills`, walking up from the current directory to the git worktree | only the nearest `AGENTS.md`, walking up from the current directory; `CLAUDE.md` only when there is no `AGENTS.md` | skill names must be unique across all locations | https://opencode.ai/docs/skills/, https://opencode.ai/docs/rules/ |

Frontmatter that every tool accepts: `name` and `description`; Codex, Cursor and OpenCode require both, Claude Code treats them as optional. Use the folder name as `name`; Cursor and OpenCode require that. Other keys are tool specific; do not use them.

The layout in `placement.md` follows from the table. `.agents/skills` serves Codex, Cursor and OpenCode; the `.claude -> .agents` symlink serves Claude Code. `AGENTS.md` holds the text and `CLAUDE.md` includes it. The link line in the root `AGENTS.md` serves every tool for sessions that start at the repo root.

## Forms that only some tools read

Use them only after the human chose "keep the limit" (Rule 7).

| Form | Read by | Source |
|---|---|---|
| `.claude/rules/*.md`, also with `paths:` globs | Claude Code | https://code.claude.com/docs/en/memory |
| `.cursor/rules` | Cursor | https://cursor.com/docs/context/rules |
| `AGENTS.override.md`, `agents/openai.yaml` | Codex | https://learn.chatgpt.com/docs/agent-configuration/agents-md, https://learn.chatgpt.com/docs/build-skills |
| `opencode.json` permissions for skills | OpenCode | https://opencode.ai/docs/skills/ |
| Frontmatter `paths`, `disable-model-invocation` | Claude Code, Cursor | https://code.claude.com/docs/en/skills, https://cursor.com/docs/skills |
| `RULES.md`, `rules/*.md` reached by a link | every tool, but only when the agent follows the link | this repo, for example `ydb/core/persqueue/RULES.md` |

## Known limits

- Two paths, one skill. Cursor and OpenCode read both `.claude/skills` and `.agents/skills`, so the symlink shows one skill under two paths, and OpenCode's documentation says to keep skill names unique across locations. If a tool lists a skill twice or refuses it, remove the symlink and keep the other three parts; Claude Code then finds the skill by the path written in `AGENTS.md`.
- Symlinks on Windows. Git checks out a symlink as a plain text file when `core.symlinks` is false (https://git-scm.com/docs/git-config#Documentation/git-config.txt-coresymlinks), and creating one needs administrator rights or Developer Mode (https://code.claude.com/docs/en/memory). Then Claude Code reaches the skill only by the path in `AGENTS.md`. `scaffold.py --no-symlink` skips the symlink.
- The `.gitignore` of this repo ignores directories named `.claude`, not symlinks named `.claude` (`.gitignore` line `.claude/`; a trailing slash matches directories only, see https://git-scm.com/docs/gitignore). `check.py` reports any file of a skill that git ignores.
- The root `.claude/` of a checkout is a real local directory (Claude Code stores local settings there; `ls -la .claude` shows it), so the root cannot have the symlink. A root skill is reached only through the link in the root `AGENTS.md`.

## Test that a tool sees the files

Run from the directory that holds the new `AGENTS.md`.

```bash
# Claude Code: the skill is in the list; expect 1 or more (the model writes the list)
claude -p "Print the names of all skills available to you, one per line." | grep -c '<skill-name>'

# Codex: the skill entry is in the prompt; grep a phrase from the description, because the
# name also appears in AGENTS.md link text; expect 1 or more
codex debug prompt-input "hi" | grep -c '<a phrase from the description>'

# OpenCode: the skill is listed once; expect 1
opencode debug skill | grep -c '"name": "<skill-name>"'
```

Cursor has no command line check here. Open the repo in Cursor, edit a file in the directory, and confirm the skill is listed once.
