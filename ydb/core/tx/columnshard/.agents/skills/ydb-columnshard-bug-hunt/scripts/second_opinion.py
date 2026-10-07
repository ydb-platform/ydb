#!/usr/bin/env python3
"""Ask a reviewer from another harness or model, as recorded in the env file, and save its final answer.

Usage:
    python3 second_opinion.py --main HARNESS=MODEL --prompt-file FILE --cwd REPO --out FILE
                              [--reviewer ID] [--env PATH] [--timeout SECONDS]

The script puts FRAMING in front of the prompt, runs the reviewer read-only in REPO, writes the final
answer to --out and the raw output to --out + ".raw".
--main is the harness and model of the current session. The env file records which reviewers equal the
session it was made in; when --main differs from that session, the script stops and asks for a new setup.
Exit codes: 0 answer saved; 2 no usable reviewer, or the env file belongs to another session;
3 the reviewer refused the prompt; 4 the reviewer failed, was not found, or timed out.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile

DEFAULT_ENV = os.path.join("~", ".config", "ydb-columnshard-bug-hunt", "env.json")

FRAMING = (
    "Context: this is quality assurance of our own open-source database, YDB. We review recently merged "
    "commits for correctness bugs (object lifetime, concurrency, crashes, wrong results, memory accounting) "
    "and write failing regression tests, because the authors of the commits have the context to fix them. "
    "This is not security research or penetration testing: do not write exploits; describe defects, the "
    "code path, and a test scenario through the public SQL API. Do not modify files.\n\n"
)

REFUSAL_PATTERNS = [
    r"flagged for possible cybersecurity risk",
    r"cyber permissive safeguards",
    r"I can(?:no|')t (?:help|assist) with",
    r"I won't be able to help",
]


def load_env(path):
    with open(os.path.expanduser(path), encoding="utf-8") as handle:
        return json.load(handle)


def env_matches_session(env, main):
    recorded = env.get("main") or {}
    return (recorded.get("harness"), recorded.get("model")) == main


def pick_reviewer(env, reviewer_id):
    usable = [r for r in env.get("reviewers", []) if r.get("ok") and not r.get("same_as_main")]
    if reviewer_id:
        for reviewer in usable:
            if reviewer["id"] == reviewer_id:
                return reviewer
        return None
    default = env.get("default_reviewer")
    for reviewer in usable:
        if reviewer["id"] == default:
            return reviewer
    return usable[0] if usable else None


def strip_ansi(text):
    return re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", text)


def is_refusal(text):
    return any(re.search(pattern, text, re.IGNORECASE) for pattern in REFUSAL_PATTERNS)


def build_command(reviewer, cwd, last, prompt):
    values = {"model": reviewer["model"], "cwd": cwd, "last": last, "prompt": prompt}
    return [part.format(**values) for part in reviewer["argv"]]


def ask(reviewer, cwd, prompt, timeout):
    """Return (status, final, raw); status is "ok", "refused" or "failed"."""
    with tempfile.TemporaryDirectory() as tmp:
        last = os.path.join(tmp, "last.txt")
        argv = build_command(reviewer, cwd, last, prompt)
        try:
            proc = subprocess.run(argv, input=prompt if reviewer["stdin"] else None, cwd=cwd,
                                  capture_output=True, text=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            return "failed", "", "timeout after {} s".format(timeout)
        except OSError as error:
            return "failed", "", "cannot run {}: {}".format(argv[0], error)
        raw = strip_ansi(proc.stdout + "\n--- stderr ---\n" + proc.stderr)
        final = proc.stdout
        if reviewer["final"] == "last_file":
            final = ""
            if os.path.exists(last):
                with open(last, encoding="utf-8") as handle:
                    final = handle.read()
        final = strip_ansi(final).strip()
    if is_refusal(final) or (not final and is_refusal(raw)):
        return "refused", final, raw
    if proc.returncode != 0 or not final:
        return "failed", final, raw
    return "ok", final, raw


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--main", required=True, help="HARNESS=MODEL of the current session")
    parser.add_argument("--prompt-file", required=True)
    parser.add_argument("--cwd", required=True, help="repository worktree the reviewer reads")
    parser.add_argument("--out", required=True)
    parser.add_argument("--reviewer", help="reviewer id from the env file; default: default_reviewer")
    parser.add_argument("--env", default=DEFAULT_ENV)
    parser.add_argument("--timeout", type=int, default=5400)
    args = parser.parse_args(argv)

    try:
        env = load_env(args.env)
    except (OSError, ValueError) as error:
        print("cannot read env file {}: {}; run the ydb-columnshard-bug-hunt-setup skill".format(args.env, error))
        return 2
    main_pair = tuple(part.strip() for part in args.main.split("=", 1))
    if not env_matches_session(env, main_pair):
        print("{} was made for session {}; run the ydb-columnshard-bug-hunt-setup skill again with --main {}".format(
            args.env, env.get("main"), args.main))
        return 2
    reviewer = pick_reviewer(env, args.reviewer)
    if reviewer is None:
        print("no usable reviewer in {}; use a fresh subagent of the main harness instead".format(args.env))
        return 2

    with open(args.prompt_file, encoding="utf-8") as handle:
        prompt = FRAMING + handle.read()
    status, final, raw = ask(reviewer, os.path.abspath(args.cwd), prompt, args.timeout)
    with open(args.out + ".raw", "w", encoding="utf-8") as handle:
        handle.write(raw)
    with open(args.out, "w", encoding="utf-8") as handle:
        handle.write(final + "\n")
    print("{} {} -> {}".format(reviewer["id"], status, args.out))
    return {"ok": 0, "refused": 3, "failed": 4}[status]


if __name__ == "__main__":
    sys.exit(main())
