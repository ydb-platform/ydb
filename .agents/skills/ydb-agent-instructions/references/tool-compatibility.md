# How the tools find instructions

Every row comes from the linked documentation. When you change this file, read the documentation again. The tools are the ones YDB contributors use.

| Tool | Reads skills from | Reads rules from | Nested directories | Source |
|---|---|---|---|---|
| Claude Code | `.claude/skills/<name>/SKILL.md`; `.agents/skills` is not a documented location | `CLAUDE.md` and `.claude/rules/*.md`; the line `@./AGENTS.md` includes `AGENTS.md` | nested `.claude/skills` and `CLAUDE.md` load when the agent works on files in that directory | https://code.claude.com/docs/en/skills, https://code.claude.com/docs/en/memory |
| Codex | `.agents/skills` in every directory from the current directory up to the repo root | `AGENTS.md` from the repo root down to the current directory, joined; stops adding files at 32 KiB | `AGENTS.md` below the current directory is not loaded | https://learn.chatgpt.com/docs/build-skills, https://learn.chatgpt.com/docs/agent-configuration/agents-md |
| Cursor | `.agents/skills`, `.cursor/skills`, `.claude/skills`, `.codex/skills`, in any subdirectory | `AGENTS.md` in the root and in subdirectories; nested files add to the parent ones | a nested skill is shown when the agent works under that directory | https://cursor.com/docs/skills, https://cursor.com/docs/context/rules |
| OpenCode | `.opencode/skills`, `.claude/skills`, `.agents/skills`, walking up from the current directory to the git worktree | only the nearest `AGENTS.md`, walking up from the current directory; `CLAUDE.md` only when there is no `AGENTS.md` | skill names must be unique across all locations | https://opencode.ai/docs/skills/, https://opencode.ai/docs/rules/ |
| GitHub Copilot | `.github/skills`, `.claude/skills`, `.agents/skills` | `AGENTS.md` anywhere in the repo, the nearest one wins; `.github/copilot-instructions.md`; a root `CLAUDE.md` or `GEMINI.md` | the nearest `AGENTS.md` in the directory tree | https://docs.github.com/en/copilot/concepts/agents/about-agent-skills, https://docs.github.com/en/copilot/how-tos/configure-custom-instructions/add-repository-instructions |
| Gemini CLI | `.gemini/skills` or its alias `.agents/skills` | `GEMINI.md` by default; `AGENTS.md` only when `context.fileName` in `settings.json` lists it; `@file.md` imports | context files load from the root and from subdirectories on demand | https://geminicli.com/docs/cli/skills/, https://geminicli.com/docs/cli/gemini-md/ |

Frontmatter that every tool accepts: `name` and `description`; Codex, Cursor and OpenCode require both, Claude Code treats them as optional. Use the folder name as `name`; Cursor and OpenCode require that.

Other keys are read by some tools and ignored by the rest, so add them when a tool works better with them. A key costs no context: tools parse the frontmatter and show only the description. Claude Code and OpenCode document that unknown keys are ignored; Codex and OpenCode were checked with a skill that carried `allowed-tools`, `paths` and `metadata` and still listed it. When two tools name the same thing differently, set both.

| Key | Meaning | Read by | Source |
|---|---|---|---|
| `allowed-tools` | tools pre-approved while the skill runs, space-separated | Claude Code; Agent Skills specification (experimental) | https://code.claude.com/docs/en/skills, https://agentskills.io/specification |
| `paths` | globs of files the skill applies to | Claude Code, Cursor | https://code.claude.com/docs/en/skills, https://cursor.com/docs/skills |
| `disable-model-invocation` | only the user may start the skill | Claude Code, Cursor; Codex uses `policy.allow_implicit_invocation: false` in `agents/openai.yaml` | https://cursor.com/docs/skills, https://learn.chatgpt.com/docs/build-skills |
| `model`, `context`, `agent`, `hooks`, `effort` | Claude Code run options | Claude Code | https://code.claude.com/docs/en/skills |
| `license`, `compatibility`, `metadata` | notes for humans and tools | Agent Skills specification, OpenCode, Cursor (`metadata`) | https://agentskills.io/specification, https://opencode.ai/docs/skills/ |

The layout in `placement.md` follows from the table. `.agents/skills` serves every tool except Claude Code. `AGENTS.md` holds the text; `CLAUDE.md` includes it and lists every skill of the directory with its path and description, so Claude Code loads both when it works on files in that directory and then reads the matching `SKILL.md`. A line that only names the `.agents/skills` directory is not enough: a small model then picks a skill from another directory. Symlinks are not used: they break repository synchronization. Gemini CLI reads `AGENTS.md` only when the user configured it; there is no repo-wide `GEMINI.md`.

## Forms that only some tools read

Use them only after the human chose "keep the limit" (Rule 7).

| Form | Read by | Source |
|---|---|---|
| `.claude/rules/*.md`, also with `paths:` globs | Claude Code | https://code.claude.com/docs/en/memory |
| `.cursor/rules` | Cursor | https://cursor.com/docs/context/rules |
| `AGENTS.override.md`, `agents/openai.yaml` | Codex | https://learn.chatgpt.com/docs/agent-configuration/agents-md, https://learn.chatgpt.com/docs/build-skills |
| `opencode.json` permissions for skills | OpenCode | https://opencode.ai/docs/skills/ |
| `.github/copilot-instructions.md`, `.github/instructions/*.instructions.md` with `applyTo` globs, `.github/skills` | GitHub Copilot | https://docs.github.com/en/copilot/how-tos/configure-custom-instructions/add-repository-instructions |
| `GEMINI.md`, `.gemini/skills` | Gemini CLI | https://geminicli.com/docs/cli/gemini-md/, https://geminicli.com/docs/cli/skills/ |
| Frontmatter `paths`, `disable-model-invocation` | Claude Code, Cursor | https://code.claude.com/docs/en/skills, https://cursor.com/docs/skills |
| `RULES.md`, `rules/*.md` reached by a pointer | every tool, but only when the agent follows the pointer | this repo, for example `ydb/core/persqueue/RULES.md` |

## Known limits

- Codex stops adding `AGENTS.md` files once the text from the root to the current directory reaches 32 KiB; a nested file past that point is dropped whole. `check.py` reports the total.
- Directories named `.claude` are ignored by git in this repo (`.gitignore` line `.claude/`), so nothing under them can be committed; that is why skills live in `.agents/skills` and Claude Code reaches them through `CLAUDE.md`.

## Test that a tool sees the files

Run from the directory that holds the new `AGENTS.md`.

```bash
# Claude Code: the pointer reaches the model; expect the sentence from AGENTS.md
claude -p "Quote the line of your project instructions that names a SKILL.md file."

# Codex: the skill entry is in the prompt (documented at https://learn.chatgpt.com/docs/developer-commands);
# grep a phrase from the description, because the name also appears in AGENTS.md; expect 1 or more
codex debug prompt-input "hi" | grep -c '<a phrase from the description>'

# OpenCode: the skill is listed once (the command is described by `opencode debug --help`); expect 1.
# Write to a file first: on a pipe OpenCode 1.2 exits before the output is flushed and the JSON is cut off
opencode debug skill > skills.json; grep -c '"name": "<skill-name>"' skills.json; rm skills.json
```

Cursor, Copilot and Gemini CLI have no command line check here. Open the repo in the tool, edit a file in the directory, and confirm the skill is listed once.
