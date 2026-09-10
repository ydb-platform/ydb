---
name: ydb-agent-instructions
description: "Read before you add or change instructions for AI coding agents in this repo: AGENTS.md, CLAUDE.md, .agents/skills/*/SKILL.md, RULES.md, rules/*.md, or files in ydb/agents/. It says where to put an instruction, how to keep it minimal and free of duplicates, how to make it work in Claude Code, Codex, Cursor and OpenCode, and how to verify every command and link. Not for user documentation. Not for code changes."
---

# How to add or change instructions for AI agents

Paths are relative to the repo root.

## Rules

1. Put an instruction in the directory of the code it is about, not higher.
2. Before you change a file, read the closest `AGENTS.md` walking up from that file, and the `SKILL.md` of any `.agents/skills/*` next to those `AGENTS.md` whose `description` matches your task. Tools do not do this for you: Codex loads `AGENTS.md` only from the repo root down to the start directory, OpenCode only the nearest one.
3. `CLAUDE.md` holds only `@./AGENTS.md`; the text goes to `AGENTS.md`. Link to rules that already exist; do not copy them.
4. Plain English: short sentences, numbered steps, exact paths. No marketing words such as "powerful" or "seamless".
5. Minimum text. Every sentence must change what an agent does. If removing a sentence changes nothing, remove it.
6. Change only what you were asked to change. On a contradiction, see Step 1.
7. Every form in this skill works in all four tools. If the human asks for a form that only some tools read (list in references/tool-compatibility.md), name the tools that will not see it and ask: make it universal, or keep the limit?

## Step 1. Find existing instructions

```bash
python3 ydb/agents/.agents/skills/ydb-agent-instructions/scripts/find.py "<your topic>"
```

It lists every instruction file and the README and contributor docs that mention the topic. Read them. If a matching instruction exists, improve it or link to it; do not write a second one.

If a found instruction contradicts what you were asked to do, stop before you change anything. Show the human both texts and ask: (1) continue with the task as asked? (2) fix, delete, or keep the old instruction? Do not edit the old instruction on your own, and do not ignore it.

Keep the list of what you found for Step 9.

## Step 2. Choose the place

- The rule is about one directory: `<dir>/AGENTS.md` or `<dir>/.agents/skills/<name>/`.
- The rule is about the whole repo (build, tests, style): `ydb/agents/<FILE>.md`, linked from `ydb/agents/GUIDE.md`.
- The root `AGENTS.md` gets at most one link line. Write the reason in the pull request.

Decision table, budgets and the layout of a skill directory: references/placement.md.

## Step 3. Choose the form

- One or two sentences that always apply to a directory: a line in `<dir>/AGENTS.md`.
- A task with steps: a skill. The `AGENTS.md` next to it gets one line that points to the skill.
- Long tables or background: references/*.md inside the skill.
- A step goes into `scripts/<name>.py` when you can write down its exact input and its exact expected output, and the same input must always give the same output. A step stays with the model only when it needs judgment about meaning: what a text means, or which of several valid options fits.
- Exception: a step that runs a few times a year and would need a script longer than `SKILL.md` may stay with the model; write the reason in `SKILL.md`.
- Scripts: Python 3.8 syntax, standard library only, tests in `scripts/tests/test_<name>.py` (`unittest`).

Example of the text: `ydb/core/blobstorage/pdisk/AGENTS.md` and `ydb/core/blobstorage/pdisk/.agents/skills/ydb-pdisk-development/SKILL.md`. That directory has no `CLAUDE.md` and no `.claude` symlink; Step 4 adds them for a new skill.

## Step 4. Create the files with the script

```bash
python3 ydb/agents/.agents/skills/ydb-agent-instructions/scripts/scaffold.py <dir> <skill-name> --description "<one sentence>"
```

`<skill-name>` starts with `ydb-` and must not exist elsewhere in the repo; the script refuses a taken name. When it warns that the name looks like another skill, tell the human and propose two or three other names before you continue.

It creates `<dir>/.agents/skills/<skill-name>/SKILL.md`, `<dir>/AGENTS.md` (or prints the line to add), `<dir>/CLAUDE.md` with `@./AGENTS.md`, and the symlink `<dir>/.claude -> .agents`. It never overwrites a file. `--dry-run` shows the plan. Why each file exists: references/tool-compatibility.md.

## Step 5. Write the text

- `description`: name the directory or component, the task types, and what the skill is not for.
- A skill about a code directory has three sections. "Source map": a table of the main files and what each one does. "Trace the changed contract": what to read on both sides of a change and which invariants to keep. "Validation": the smallest test target to run first.
- Write paths as plain text, relative to the repo root or to the file; no markdown links.
- Replace every `TODO` line the script created.

## Step 6. Verify every command and claim

For each command in the text you wrote or changed:

1. Check the flags against the tool's help or documentation.
2. Run the command when the run is safe and light: a help command, a dry run, a build of one small target. Do not run heavy tests only to check a document.
3. If you cannot run it (tool missing, needs credentials, too heavy): do not mark it as verified. List it in the report and ask the human to run it.

For each claim about a tool ("Codex loads X"): read the tool's documentation and keep the URL. If you cannot confirm the claim, remove it.

For each script you changed: run its tests before and after the change.

Commands, levels of verification and the report template: references/verification.md.

## Step 7. Run the checker

```bash
python3 ydb/agents/.agents/skills/ydb-agent-instructions/scripts/check.py <dir>
```

Add the root `AGENTS.md` to the command when you changed it. Fix every `ERROR` line. Read each `WARN` line and fix it or explain it in the report.

## Step 8. Independent review

Ask a separate agent to review your files as described in references/verification.md: fresh context, the reviewer prompt from that file, no notes from you.

For each finding: restate it in your own words, check it against the source, then fix it or answer with a technical reason. Do not agree just to be polite.

## Step 9. Report

Fill the report template from references/verification.md.
