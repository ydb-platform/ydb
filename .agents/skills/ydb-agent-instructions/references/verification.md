# Verify before you finish

## Three levels

| Level | Meaning | Write in the report |
|---|---|---|
| Ran | you executed the command and saw the expected result | the command and one line of output |
| Read | you read the tool's documentation or help output and it confirms the flag or claim | the URL or the help line |
| Not verified | you could not run or read it | the reason and the exact command the human should run |

## Safe commands

Run these freely: help and version commands; dry runs (`git add -n`, `create_skill.py --dry-run`); script tests; a build of one small target (`./ya make --build relwithdebinfo <small folder>`); the discovery commands from `tool-compatibility.md`.

Do not run these only to check a document: test runs with `-tA` of large folders; anything that needs credentials or a cluster; anything that writes outside your change. The first `./ya` run downloads the tool into the home directory; that is allowed.

## How to check a `ya` flag

```bash
./ya make -hh | grep -- '--test-retries'
```

The short `./ya make --help` hides most flags; use `-hh` or `-hhh`.

## How to check a script

Every contributor has Python 3 because `./ya` needs it; nothing else is guaranteed. Scripts use Python 3.9 syntax, the version macOS Command Line Tools ship (`/usr/bin/python3 --version`). Standard library only: no `match`, no `X | Y` in type hints, no parenthesized `with` items, no pip packages. On Windows the commands below start with `py -3` instead of `python3.9` or `python3`. `check.py` rejects grammar newer than 3.9 and imports that the running interpreter does not find in its standard library; the run under `python3.9` is the real test.

```bash
python3.9 -m unittest discover -s <skill>/scripts/tests
python3 -m unittest discover -s <skill>/scripts/tests       # also on the newest Python
grep -n -E '^(import|from) ' <script>                       # every module must be in the standard library
```

## Report template

```text
Changed files: ...
Existing instructions found (Step 1) and what was done with each: ...
Verified by running: ...
Verified by reading documentation: ...
Not verified, and what the human should run: ...
Review findings and what happened to each: ...
```

## Independent review

Run the review as a separate agent in a fresh context. Do not show it your notes or the result you expect.

- Claude Code: the Agent tool.
- Codex: `codex exec -s read-only "<prompt>"`.
- OpenCode: `opencode run "<prompt>"`.
- No separate agent available: do a second pass in a new session and write in the report that the review was not independent.

### Reviewer prompt

Copy this text, then add the list of files.

```text
You review instruction files for AI coding agents in the YDB repository.
Default stance: reject each item unless you can confirm it yourself from the files or documentation.
Do not trust the author. Do not guess. Read every file in the list and every file it links to.

Check and report:
1. Steps. Can a mid-size model follow every step without extra knowledge? Name each step that needs a guess.
2. Minimum. For each sentence and section, ask whether removing it would change what an agent does. Name each one that can be removed.
3. Duplication. Run python3 .agents/skills/ydb-agent-instructions/scripts/find.py "<topic>" and read the listed files. Name each overlap and each contradiction.
4. Placement. Is each file in the narrowest directory that fits? Name each file that could move closer to the code.
5. Links and paths. Resolve each relative link and each path in text. Name each one that does not exist.
6. Commands. Run each command that is safe: help commands, dry runs, and
   python3 .agents/skills/ydb-agent-instructions/scripts/check.py <dir>
   Name each command that fails or that you could not run.
7. Scripts. For each scripts/<name>.py run
   python3.9 -m unittest discover -s <skill>/scripts/tests   (or python3 when 3.9 is absent)
   grep -n -E '^(import|from) ' <script>
   Name each failing test and each import outside the standard library. Name each step in the text that a script could do.
8. Trigger. Give three requests where the skill must be used and three where it must not. State whether the description picks the right ones.
9. Language. Name each sentence that is long, vague, or uses words a reader must guess.

Output one finding per line:
file:line | error or warning | what is wrong | evidence
End with the exact commands you ran.

Files:
```
