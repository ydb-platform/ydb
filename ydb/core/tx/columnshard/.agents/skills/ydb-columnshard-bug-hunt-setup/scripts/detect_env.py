#!/usr/bin/env python3
"""Detect agent harnesses and models that can review bug-hunt findings, and write the env file.

Usage:
    python3 detect_env.py --main HARNESS=MODEL [--model HARNESS=MODEL ...] [--subagent-model NAME ...]
                          [--out PATH] [--timeout SECONDS] [--skip-smoke]

--main     the harness and model of the current session; a reviewer must differ from it.
--model    an extra model to probe; repeat the flag for more models.
--subagent-model  a model the subagent tool of the current harness offers; recorded, not probed.
--out      the env file; default ~/.config/ydb-columnshard-bug-hunt/env.json.
--skip-smoke  print the candidates without running them; nothing is written.

For every harness found in PATH the script collects candidate models from the harness config
and from --model, runs one short prompt per model, and records which ones answered.
The env file lists every probed reviewer with its command template and the result.
"""

import argparse
import datetime
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile

SMOKE_PROMPT = "Reply with the single word OK."
DEFAULT_OUT = os.path.join("~", ".config", "ydb-columnshard-bug-hunt", "env.json")

# Command templates. {model}, {cwd}, {last} and {prompt} are replaced when the command runs.
# "stdin": the prompt goes to standard input. "final": where the final answer is read from.
HARNESSES = {
    "codex": {
        "argv": ["codex", "exec", "-m", "{model}", "-s", "read-only", "-C", "{cwd}",
                 "--skip-git-repo-check", "-o", "{last}", "-"],
        "stdin": True,
        "final": "last_file",
    },
    "claude": {
        "argv": ["claude", "-p", "--model", "{model}", "--allowedTools",
                 "Read,Grep,Glob,Bash(git show:*),Bash(git log:*),Bash(git diff:*),Bash(git grep:*),"
                 "Bash(arc show:*),Bash(arc log:*),Bash(arc diff:*)",
                 "--disallowedTools", "Edit,Write,NotebookEdit"],
        "stdin": True,
        "final": "stdout",
    },
    "opencode": {
        "argv": ["opencode", "run", "-m", "{model}", "{prompt}"],
        "stdin": False,
        "final": "stdout",
    },
}

# Models probed even when no config names them. The opencode ids are the usual provider ids for GLM and Kimi;
# pass --model opencode=<provider/model> when your provider names them differently.
DEFAULT_MODELS = {
    "claude": ["opus", "sonnet"],
    "opencode": ["zhipuai/glm-5.3", "moonshotai/kimi-k2.6"],
}

# The default reviewer is the first usable id in this order, then any other usable reviewer.
PREFERRED_REVIEWERS = ["claude/opus"]


def parse_pair(text):
    if "=" not in text:
        raise ValueError("expected HARNESS=MODEL, got: " + text)
    harness, model = text.split("=", 1)
    return harness.strip(), model.strip()


def codex_config_models(text):
    """Return the models named in a codex config.toml, top level and profiles, in file order."""
    models = []
    for line in text.splitlines():
        match = re.match(r'^\s*model\s*=\s*"([^"]+)"', line)
        if match and match.group(1) not in models:
            models.append(match.group(1))
    return models


def json_config_model(text, path):
    """Return the model in a JSON config; path is a list of keys, the last value may be a dict with "name"."""
    try:
        value = json.loads(text)
    except ValueError:
        return None
    for key in path:
        if not isinstance(value, dict) or key not in value:
            return None
        value = value[key]
    if isinstance(value, dict):
        value = value.get("name")
    return value if isinstance(value, str) and value else None


def read_text(path):
    try:
        with open(os.path.expanduser(path), encoding="utf-8") as handle:
            return handle.read()
    except OSError:
        return None


def config_models(harness):
    if harness == "codex":
        text = read_text("~/.codex/config.toml")
        return codex_config_models(text) if text else []
    if harness == "opencode":
        text = read_text("~/.config/opencode/opencode.json")
        model = json_config_model(text, ["model"]) if text else None
        return [model] if model else []
    return []


def candidate_models(harness, extra, found_in_config):
    models = []
    for model in found_in_config + DEFAULT_MODELS.get(harness, []) + extra:
        if model and model not in models:
            models.append(model)
    return models


def same_as_main(harness, model, main):
    """True when the model is the session model, also when one name is an alias contained in the other."""
    if main is None:
        return False
    left, right = model.lower(), main[1].lower()
    if harness != main[0] and "/" not in left:
        return False
    left = left.split("/")[-1]
    return left == right or left in right or right in left


def build_command(spec, model, cwd, last, prompt):
    values = {"model": model, "cwd": cwd, "last": last, "prompt": prompt}
    return [part.format(**values) for part in spec["argv"]]


def strip_ansi(text):
    return re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", text)


def run_prompt(spec, model, cwd, prompt, timeout):
    """Run one prompt; return (exit_code, final_text, stderr_tail)."""
    with tempfile.TemporaryDirectory() as tmp:
        last = os.path.join(tmp, "last.txt")
        argv = build_command(spec, model, cwd, last, prompt)
        try:
            proc = subprocess.run(argv, input=prompt if spec["stdin"] else None, cwd=cwd,
                                  capture_output=True, text=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            return 124, "", "timeout after {} s".format(timeout)
        except OSError as error:
            return 127, "", str(error)
        final = proc.stdout
        if spec["final"] == "last_file":
            final = read_text(last) or ""
        return proc.returncode, strip_ansi(final).strip(), strip_ansi(proc.stderr)[-400:]


def smoke_ok(code, final):
    return code == 0 and re.fullmatch(r"\W*ok\W*", final.strip(), re.IGNORECASE) is not None


def detect(main, extra_models, which, runner, timeout, skip_smoke, cwd, subagent_models=()):
    reviewers = []
    for harness, spec in HARNESSES.items():
        if which(spec["argv"][0]) is None:
            continue
        extra = [model for name, model in extra_models if name == harness]
        for model in candidate_models(harness, extra, config_models(harness)):
            entry = {
                "id": harness + "/" + model,
                "harness": harness,
                "model": model,
                "argv": spec["argv"],
                "stdin": spec["stdin"],
                "final": spec["final"],
                "same_as_main": same_as_main(harness, model, main),
                "ok": None,
                "error": "",
            }
            if not skip_smoke and not entry["same_as_main"]:
                code, final, stderr = runner(spec, model, cwd, SMOKE_PROMPT, timeout)
                entry["ok"] = smoke_ok(code, final)
                if not entry["ok"]:
                    entry["error"] = (final or stderr)[-400:]
            reviewers.append(entry)
    usable = [r for r in reviewers if r["ok"] and not r["same_as_main"]]
    preferred = [r for pid in PREFERRED_REVIEWERS for r in usable if r["id"] == pid]
    other_harness = [r for r in usable if main is None or r["harness"] != main[0]]
    default = (preferred or other_harness or usable or [None])[0]
    return {
        "version": 1,
        "created": datetime.datetime.now().isoformat(timespec="seconds"),
        "main": {"harness": main[0], "model": main[1]} if main else None,
        "subagent_models": list(subagent_models),
        "reviewers": reviewers,
        "default_reviewer": default["id"] if default else None,
    }


def describe(reviewer):
    if reviewer["same_as_main"]:
        return "same as main"
    if reviewer["ok"] is None:
        return "not probed"
    return "ok" if reviewer["ok"] else "failed"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--main", required=True, help="HARNESS=MODEL of the current session")
    parser.add_argument("--model", action="append", default=[], help="extra HARNESS=MODEL to probe")
    parser.add_argument("--subagent-model", action="append", default=[], help="model offered by the subagent tool")
    parser.add_argument("--out", default=DEFAULT_OUT)
    parser.add_argument("--timeout", type=int, default=240)
    parser.add_argument("--skip-smoke", action="store_true", help="list candidates without running them")
    args = parser.parse_args(argv)

    env = detect(parse_pair(args.main), [parse_pair(m) for m in args.model], shutil.which, run_prompt,
                 args.timeout, args.skip_smoke, os.getcwd(), args.subagent_model)
    out = os.path.abspath(os.path.expanduser(args.out))
    if not args.skip_smoke:
        os.makedirs(os.path.dirname(out), exist_ok=True)
        with open(out, "w", encoding="utf-8") as handle:
            json.dump(env, handle, indent=2)
    for reviewer in env["reviewers"]:
        state = describe(reviewer)
        print("{:40} {}{}".format(reviewer["id"], state, ": " + reviewer["error"][:120] if reviewer["error"] else ""))
    print("default reviewer: {}".format(env["default_reviewer"]))
    print("not written (--skip-smoke)" if args.skip_smoke else "written: {}".format(out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
