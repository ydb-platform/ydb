---
name: ydb-agent-instructions
description: "Use before you add or change instructions for AI agents: AGENTS.md, CLAUDE.md, skills, rules."
---

# How to add or change instructions for AI agents

Paths are relative to the repo root.

## Rules

1. Put an instruction in the directory of the code it is about, not higher. The root `AGENTS.md` and the root `.agents/skills` get only what every session needs, and only with a written reason.
2. Before you change a file, read the closest `AGENTS.md` walking up from that file, and the `SKILL.md` of any `.agents/skills/*` next to those `AGENTS.md` whose `description` matches your task. Tools do not do this for you: Codex loads `AGENTS.md` only from the repo root down to the start directory, OpenCode only the nearest one.
3. `AGENTS.md` holds the text. `CLAUDE.md` holds `@./AGENTS.md` and one line per skill with its path and description, because Claude Code does not read `.agents/skills` on its own. Point to rules that already exist; do not copy them.
4. Plain English: short sentences, numbered steps, exact paths. No marketing words such as "powerful" or "seamless".
5. Minimum text. Every sentence must change what an agent does. If removing a sentence changes nothing, remove it.
6. Change only what you were asked to change. On a contradiction, see Step 1.
7. Every form in this skill works in all the tools listed in references/tool-compatibility.md. Frontmatter keys that only some tools read are fine: the others ignore them. If the human asks for a file or layout that only some tools read (table in that file), name the tools that will not see it and ask: make it universal, or keep the limit?

## Step 1. Find existing instructions

```bash
python3 .agents/skills/ydb-agent-instructions/scripts/find.py "<your topic>"
```

It lists every instruction file and the README and contributor docs that mention the topic. Read them. If a matching instruction exists, improve it or point to it; do not write a second one.

If a found instruction contradicts what you were asked to do, stop before you change anything. Show the human both texts and ask: (1) continue with the task as asked? (2) fix, delete, or keep the old instruction? Do not edit the old instruction on your own, and do not ignore it.

Keep the list of what you found for Step 9.

## Step 2. Choose the place

- The rule is about one directory: `<dir>/AGENTS.md` or `<dir>/.agents/skills/<name>/`.
- The rule is about the whole repo (build, tests, style): the root `AGENTS.md`. When the text is long, put it in a separate doc next to the shared docs Step 1 found, and add one line in the root `AGENTS.md` that says what the doc is and when to read it.
- The rule is needed in every session, in every directory: a skill in the root `.agents/skills` or one line in the root `AGENTS.md`. Write the reason in the pull request.

Decision table, budgets and the layout of a skill directory: references/placement.md.

## Step 3. Choose the form

- One or two sentences that always apply to a directory: a line in `<dir>/AGENTS.md`.
- A task with steps: a skill.
- Long tables or background: references/*.md inside the skill.
- A step goes into `scripts/<name>.py` when you can write down its exact input and its exact expected output, and the same input must always give the same output. A step stays with the model only when it needs judgment about meaning: what a text means, or which of several valid options fits.
- Exception: a step that runs a few times a year and would need a script longer than `SKILL.md` may stay with the model; write the reason in `SKILL.md`.
- Scripts: Python 3.9 syntax, standard library only, tests in `scripts/tests/test_<name>.py` (`unittest`).

Example of the text: `ydb/core/blobstorage/pdisk/.agents/skills/ydb-pdisk-development/SKILL.md`.

## Step 4. Choose the name and create the files

The name starts with `ydb-` and must be unique in the repo. Print the existing skill names with their directories and the component directories, and compare:

```bash
python3 .agents/skills/ydb-agent-instructions/scripts/find.py --skills
```

If a reader could take the new name for an existing skill, or for a component the skill does not cover, tell the human and propose two or three other names before you continue.

```bash
python3 .agents/skills/ydb-agent-instructions/scripts/create_skill.py <dir> <skill-name> --description "<one sentence>"
```

It creates `<dir>/.agents/skills/<skill-name>/SKILL.md`, `<dir>/AGENTS.md` when absent, and `<dir>/CLAUDE.md` with `@./AGENTS.md` plus one line per skill. When `CLAUDE.md` already exists it adds the line of the new skill and changes nothing else. Every file goes into `<dir>` itself, never into a parent: Claude Code reads the `CLAUDE.md` of the directory it works in. It never overwrites a file and refuses a name that is already taken. `--dry-run` shows the plan. Why each file exists: references/tool-compatibility.md.

## Step 5. Write the text

- `description`: one short sentence that says when to use the skill; name the directory or component and the task type.
- A skill about a code directory usually has three sections, as in `ydb-pdisk-development`. "Source map": a table of the main files and what each one does. "Trace the changed contract": what to read on both sides of a change and which invariants to keep. "Validation": the smallest test target to run first.
- Write paths as plain text, relative to the file (`.agents/skills/<name>/SKILL.md`, `../README.md`) or from the repo root (`ydb/core/blobstorage/README.md`).
- Replace every `TODO` line the script created, in `SKILL.md` and in `AGENTS.md`.

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
python3 .agents/skills/ydb-agent-instructions/scripts/check.py <dir>
```

Add the root `AGENTS.md` to the command when you changed it. Fix every `ERROR` line. Read each `WARN` line and fix it or explain it in the report.

## Step 8. Independent review

Ask a separate agent to review your files as described in references/verification.md: fresh context, the reviewer prompt from that file, no notes from you.

For each finding: restate it in your own words, check it against the source, then fix it or answer with a technical reason. Do not agree just to be polite.

## Step 9. Report

Fill the report template from references/verification.md.
