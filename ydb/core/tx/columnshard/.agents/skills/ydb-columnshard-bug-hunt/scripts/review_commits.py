#!/usr/bin/env python3
"""Ask the reviewer from the env file about every commit of the list, two prompts per commit, in parallel.

Usage:
    python3 review_commits.py --main HARNESS=MODEL --commits FILE --cwd WORKTREE --out DIR
                              [--jobs N] [--timeout SECONDS] [--env PATH] [--reviewer ID]

FILE is the output of list_commits.py. Commits with tests_only = yes are skipped.
For each commit the script writes DIR/<sha>-plain.md and DIR/<sha>-lifetime.md (plus .raw files) and
one line per prompt to DIR/summary.tsv: sha, prompt kind, status (ok, refused, failed).
A run of one prompt usually takes 10-40 minutes; the default timeout allows 90.
Exit codes: 0 all prompts ran (see summary.tsv for each status); 2 no usable reviewer or wrong session.
"""

import argparse
import concurrent.futures
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import second_opinion  # noqa: E402

PROMPTS = {
    "plain": "Find bugs in commit {sha} of the repository in {cwd} (read its diff). Report each with file:line and why it is a bug.",
    "lifetime": (
        "Code review of commit {sha} of the repository in {cwd} (read its diff) in YDB ColumnShard. Look for concurrency and "
        "object-lifetime correctness bugs: state accessed from an actor thread and from conveyor or worker threads "
        "without synchronization, objects destroyed while a queued task or callback still references them, invalid "
        "iterators or views, wrong index math, row counts of arrays and filters that do not match, allocations sized "
        "from metadata instead of the data actually used. For each: file:line, the concrete interleaving or input, "
        "confidence, and an SQL query pattern over a column table (STORE = COLUMN) that exercises it. "
        "Say \"none found\" if nothing is concrete. Under 700 words."
    ),
}


def read_commits(path):
    commits = []
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 10 or parts[7] == "yes":
                continue
            commits.append(parts[0])
    return commits


def run_all(reviewer, commits, cwd, out, jobs, timeout, ask=second_opinion.ask):
    tasks = [(sha, kind) for sha in commits for kind in PROMPTS]

    def one(task):
        sha, kind = task
        prompt = second_opinion.FRAMING + PROMPTS[kind].format(sha=sha, cwd=cwd)
        status, final, raw = ask(reviewer, cwd, prompt, timeout)
        base = os.path.join(out, "{}-{}.md".format(sha, kind))
        with open(base + ".raw", "w", encoding="utf-8") as handle:
            handle.write(raw)
        with open(base, "w", encoding="utf-8") as handle:
            handle.write(final + "\n")
        return sha, kind, status

    with concurrent.futures.ThreadPoolExecutor(max_workers=jobs) as pool:
        results = list(pool.map(one, tasks))
    with open(os.path.join(out, "summary.tsv"), "w", encoding="utf-8") as handle:
        for row in results:
            handle.write("\t".join(row) + "\n")
    return results


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--main", required=True, help="HARNESS=MODEL of the current session")
    parser.add_argument("--commits", required=True, help="output file of list_commits.py")
    parser.add_argument("--cwd", required=True, help="worktree the reviewer reads")
    parser.add_argument("--out", required=True)
    parser.add_argument("--jobs", type=int, default=8)
    parser.add_argument("--timeout", type=int, default=5400)
    parser.add_argument("--env", default=second_opinion.DEFAULT_ENV)
    parser.add_argument("--reviewer")
    args = parser.parse_args(argv)

    try:
        env = second_opinion.load_env(args.env)
    except (OSError, ValueError) as error:
        print("cannot read env file {}: {}; run the ydb-columnshard-bug-hunt-setup skill".format(args.env, error))
        return 2
    main_pair = tuple(part.strip() for part in args.main.split("=", 1))
    if not second_opinion.env_matches_session(env, main_pair):
        print("{} was made for session {}; run the ydb-columnshard-bug-hunt-setup skill again".format(args.env, env.get("main")))
        return 2
    reviewer = second_opinion.pick_reviewer(env, args.reviewer)
    if reviewer is None:
        print("no usable reviewer in {}; use fresh subagents of the main harness instead".format(args.env))
        return 2

    os.makedirs(args.out, exist_ok=True)
    commits = read_commits(args.commits)
    results = run_all(reviewer, commits, os.path.abspath(args.cwd), args.out, args.jobs, args.timeout)
    for sha, kind, status in results:
        print("{}\t{}\t{}".format(sha, kind, status))
    print("{} commits, {} prompts, reviewer {}; summary in {}".format(
        len(commits), len(results), reviewer["id"], os.path.join(args.out, "summary.tsv")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
