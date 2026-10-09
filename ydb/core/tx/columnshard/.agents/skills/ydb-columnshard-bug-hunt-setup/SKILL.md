---
name: ydb-columnshard-bug-hunt-setup
description: "Use before the first ydb-columnshard-bug-hunt run on a machine, when its env file is missing, or when the main harness or model changed: detect the agent harnesses and models available for independent reviews and write the env file."
---

# Bug hunt setup

The bug hunt (QA of our own code, not security testing) asks other harnesses or models to review its findings. This skill writes `~/.config/ydb-columnshard-bug-hunt/env.json`, which says which ones work here.

## Steps

1. Note your session as `HARNESS=MODEL`: the harness you run in (`claude`, `codex`, `opencode`) and the model id from your system prompt or session settings. Ask the user if you do not know it. Also list the models your subagent tool lets you choose, if any.
2. Ask the user which other models to try. Each one is `HARNESS=MODEL`.
3. Run from the repo root:
   ```bash
   python3 ydb/core/tx/columnshard/.agents/skills/ydb-columnshard-bug-hunt-setup/scripts/detect_env.py \
       --main <harness>=<model> [--model <harness>=<model> ...] [--subagent-model <name> ...]
   ```
   The script looks for `codex`, `claude` and `opencode` in `PATH`. It takes models from the Codex and OpenCode config files and from `--model`, and adds `opus` and `sonnet` for `claude` and GLM 5.3 and Kimi 2.6 for `opencode`. It sends each model one short prompt and records which ones answer "OK". A model that equals your own is marked `same_as_main` and is not used. Each probe can take a few minutes; `--skip-smoke` prints the candidates and writes nothing.
4. For every `failed` line tell the user the reason from the error text, for example "model not supported with this account". The user may name another model; run step 3 again with it.
5. Tell the user the default reviewer: `claude/opus` when it works and is not your own model; else a model of another harness; else any other working model. When none works, the bug hunt uses fresh subagents of your own harness.

Reviewers run read-only where the harness allows it: Codex with `-s read-only`, Claude with an allow-list of read tools and read-only `git` and `arc` commands. OpenCode has no such flag in the command; only the prompt asks it not to change files.

## Check one reviewer by hand

```bash
echo "Which function in ydb/core/formats/arrow/accessor/dictionary/accessor.cpp calls arrow::compute::Take? Answer with the function name only." > /tmp/question.txt
python3 ydb/core/tx/columnshard/.agents/skills/ydb-columnshard-bug-hunt/scripts/second_opinion.py \
    --main <harness>=<model> --prompt-file /tmp/question.txt --cwd . --out /tmp/answer.md
```

Expected: exit code 0 and `TDictionaryArray::DoGetLocalData` in `/tmp/answer.md`.
