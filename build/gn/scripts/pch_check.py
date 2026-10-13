"""Checks a precompiled header shared by many targets (pch_mode = "global",
//build/gn/pch) against the files that use it.

gn compiles the PCH from --source (a stub including the header) with the
default configs. Every C++ file whose command line (compile_commands.json) has
`-include-pch <--pch>` is a consumer. clang rejects a PCH made with another
value of a macro, but takes one made without a -D the consumer has, or with one
the consumer lacks -- the macro is then defined in the consumer. So:

  * every -D of the PCH is a -D of every consumer, with the same value;
  * the header depends on no other -D of a consumer: it is preprocessed once
    as is and once with every such macro defined to a poison token, and any
    difference fails, naming the macros.

It reruns when the compile commands or the PCH change: the depfile names both
(compile_commands.json is written by `gn gen`, no target of the build).

Usage: pch_check.py --compile-commands F --pch P --source S --stamp T
                    --depfile D [--root R]
"""

import argparse
import json
import os
import shlex
import subprocess
import sys

_DROP_WITH_ARG = {"-o", "-MF", "-MT", "-MQ", "-include-pch", "-include"}
_DROP = {"-c", "-MD", "-MMD", "-fsyntax-only"}
_POISON = "__gn_global_pch_poison__"


def argv_of(entry):
    if "arguments" in entry:
        return list(entry["arguments"])
    command = entry["command"]
    # shlex is slow (a minute for all the commands of the build): only when
    # the command does need it
    if any(c in command for c in "\"'\\"):
        return shlex.split(command)
    return command.split()


def defines(argv):
    out = []
    for i, a in enumerate(argv):
        if a == "-D" and i + 1 < len(argv):
            out.append(argv[i + 1])
        elif a.startswith("-D") and a != "-D":
            out.append(a[2:])
    return out


def name(define):
    return define.partition("=")[0]


def consumers(entries, pch):
    out = []
    pch_file = os.path.basename(pch)
    for e in entries:
        if pch_file not in (e["command"] if "command" in e else " ".join(e["arguments"])):
            continue
        argv = argv_of(e)
        for i, a in enumerate(argv[:-1]):
            if a == "-include-pch" and os.path.normpath(argv[i + 1]) == pch:
                out.append((e, argv))
                break
    return out


def base_flags(entry, argv):
    """argv of the PCH compile without its input, output, dependency file,
    forced includes and the -x of its input."""
    out, skip = [], 0
    for i, a in enumerate(argv):
        if skip:
            skip -= 1
            continue
        if a in _DROP_WITH_ARG or (a == "-x" and argv[i + 1:i + 2] == ["c++-header"]):
            skip = 1
            continue
        if a == "-Xclang" and argv[i + 1:i + 3] == ["-include", "-Xclang"]:
            skip = 3
            continue
        if a in _DROP or a == entry["file"]:
            continue
        out.append(a)
    return out


def preprocess(directory, argv, source, extra):
    proc = subprocess.run(argv + extra + ["-E", "-P", "-x", "c++-header", source],
                          cwd=directory, capture_output=True, text=True)
    return proc.returncode, proc.stdout, proc.stderr


def culprits(directory, argv, source, names, reference):
    """The macros among `names` the header depends on: bisection."""
    def differs(subset):
        rc, out, _ = preprocess(directory, argv, source, ["-D%s=%s" % (n, _POISON) for n in subset])
        return rc != 0 or out != reference

    if not names or not differs(names):
        return []
    if len(names) == 1:
        return names
    half = len(names) // 2
    return (culprits(directory, argv, source, names[:half], reference)
            + culprits(directory, argv, source, names[half:], reference))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--compile-commands", required=True)
    ap.add_argument("--pch", required=True)
    ap.add_argument("--source", required=True)
    ap.add_argument("--stamp", required=True)
    ap.add_argument("--depfile", required=True)
    ap.add_argument("--root", default=".")
    args = ap.parse_args()

    with open(args.compile_commands, encoding="utf-8") as f:
        entries = json.load(f)
    pch = os.path.normpath(args.pch)
    want = os.path.realpath(os.path.join(args.root, args.source))
    entry = next((e for e in entries
                  if os.path.realpath(os.path.join(e["directory"], e["file"])) == want), None)
    if entry is None:
        sys.exit("pch_check: no compile command of %s" % args.source)
    pch_argv = argv_of(entry)
    pch_defines = set(defines(pch_argv))
    users = consumers(entries, pch)

    lacking = {}
    names = set()
    for e, argv in users:
        ds = set(defines(argv))
        names.update(name(d) for d in ds)
        missing = pch_defines - ds
        if missing:
            lacking.setdefault(" ".join(sorted(missing)), []).append(e["file"])
    if lacking:
        sys.exit("pch_check: files using %s lack -D of the PCH (exclude their targets):\n%s" % (
            pch, "\n".join("  %s: %d files, e.g. %s" % (m, len(fs), fs[0]) for m, fs in sorted(lacking.items()))))

    directory = entry["directory"]
    argv = base_flags(entry, pch_argv)
    varying = sorted(names - {name(d) for d in pch_defines})
    rc, reference, err = preprocess(directory, argv, entry["file"], [])
    if rc != 0:
        sys.exit("pch_check: preprocessing %s failed:\n%s" % (args.source, err[-4000:]))
    bad = culprits(directory, argv, entry["file"], varying, reference)
    if bad:
        sys.exit("pch_check: %s depends on -D that differ between the files using it: %s\n"
                 "drop the headers using them from it" % (args.source, ", ".join(bad)))

    with open(args.stamp, "w", encoding="utf-8") as f:
        f.write("%d files\n" % len(users))
    with open(args.depfile, "w", encoding="utf-8") as f:
        f.write("%s: %s %s\n" % (args.stamp, args.compile_commands, args.pch))


if __name__ == "__main__":
    main()
