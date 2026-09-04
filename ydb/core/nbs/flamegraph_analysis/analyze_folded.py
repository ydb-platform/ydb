#!/usr/bin/env python3
"""Summarize a FlameGraph folded-stacks file as text.

Usage: analyze_folded.py folded.txt [--top N] [--grep SUBSTR] [--thread COMM]

Prints:
  * total samples and per-thread (comm) breakdown
  * top leaf frames by self time
  * top frames by inclusive time
  * hottest full stacks
"""
import sys
import argparse
from collections import Counter


def load(path):
    stacks = []
    with open(path) as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            stack, _, cnt = line.rpartition(" ")
            try:
                cnt = int(cnt)
            except ValueError:
                continue
            stacks.append((stack.split(";"), cnt))
    return stacks


def bar(frac, width=40):
    n = int(round(frac * width))
    return "#" * n + "." * (width - n)


def report(title, counter, total, top):
    print(f"\n=== {title} ===")
    for name, cnt in counter.most_common(top):
        print(f"{100.0*cnt/total:6.2f}%  {cnt:6d}  {bar(cnt/total, 24)}  {name}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("path")
    ap.add_argument("--top", type=int, default=30)
    ap.add_argument("--grep", default=None, help="only stacks containing this substring")
    ap.add_argument("--exclude", action="append", default=[],
                    help="drop stacks containing this substring (repeatable)")
    ap.add_argument("--thread", default=None,
                    help="only stacks whose root frame equals this value exactly")
    args = ap.parse_args()

    stacks = load(args.path)
    if args.grep:
        stacks = [(s, c) for s, c in stacks if any(args.grep in f for f in s)]
    for ex in args.exclude:
        stacks = [(s, c) for s, c in stacks if not any(ex in f for f in s)]
    if args.thread:
        stacks = [(s, c) for s, c in stacks if s[0] == args.thread]

    total = sum(c for _, c in stacks)
    if not total:
        print("no samples matched")
        return
    print(f"total samples: {total}")

    threads = Counter()
    leaves = Counter()
    inclusive = Counter()
    whole = Counter()
    for frames, cnt in stacks:
        threads[frames[0]] += cnt
        leaves[frames[-1]] += cnt
        whole[";".join(frames)] += cnt
        for f in set(frames):
            inclusive[f] += cnt

    report("threads (root frame = comm)", threads, total, args.top)
    report("self time (leaf frames)", leaves, total, args.top)
    report("inclusive time (any frame)", inclusive, total, args.top)

    print(f"\n=== hottest full stacks (top {min(args.top, 15)}) ===")
    for stack, cnt in whole.most_common(min(args.top, 15)):
        print(f"\n-- {100.0*cnt/total:.2f}%  {cnt} samples")
        for i, f in enumerate(stack.split(";")):
            print("   " + "  " * min(i, 20) + f)


if __name__ == "__main__":
    main()
