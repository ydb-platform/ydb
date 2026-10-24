#!/usr/bin/env python3
"""The targets a developer would build to check a commit, as if the commit were
one edit of an iteration (the ya side of the bench of follow_main.py):

  1. the tests the commit changes (the dirs of //:tests in the root BUILD.gn
     that hold changed files) -- the author ran them; the other files of the
     modules they test count as covered;
  2. else the tests of the module of a changed file (UNITTEST_FOR / GTEST_FOR
     in their ya.make, or the dir above ut/), if there are at most CAP of them;
  3. else those of them, whose names share a word with the changed file
     (schemeshard__import.cpp -> ut_import);
  4. a C/C++ file with no test found: its module itself (if GN builds it).

Docs (.md, ydb/docs, .github) are ignored. The test index is of the work tree
(--root), the changes are of the commit against its first parent.

    follow_select.py --root <work tree> <commit>...            # JSON per commit
    follow_select.py --root <work tree> --range A..B [--json F] # a summary
"""

import argparse
import collections
import json
import os
import re
import subprocess

CAP = 5
CXX = (".c", ".cc", ".cpp", ".cxx", ".h", ".hh", ".hpp", ".inl", ".ipp")
TEST_DIR_RE = re.compile(r"(ut|ut_.*|.*_ut|gtest|test|tests)$")
STOP_WORDS = {"ut", "cpp", "test", "tests", "impl", "core", "ydb", "lib", "src"}


def tokens(s):
    s = re.sub(r"([a-z])([A-Z])", r"\1_\2", s)
    return {w.lower() for w in re.split(r"[^A-Za-z0-9]+", s) if len(w) > 2} - STOP_WORDS


class Selector:
    def __init__(self, root):
        self.root = root
        with open(os.path.join(root, "BUILD.gn")) as f:
            text = f.read()
        m = re.search(r'^group\("tests"\)\s*\{(.*?)^\}', text, re.S | re.M)
        labels = re.findall(r'^\s*"//([^"]+)"', m.group(1), re.M) if m else []
        self.tests = set()
        self.under = collections.defaultdict(set)     # module dir -> its test dirs
        for label in labels:
            d = label.split(":")[0]
            path = os.path.join(root, d, "ya.make")
            if not os.path.exists(path):
                continue
            self.tests.add(d)
            with open(path) as f:
                m = re.search(r"^\s*(?:UNITTEST_FOR|GTEST_FOR)\s*\(\s*([^)\s]+)", f.read(), re.M)
            if m:
                mod = m.group(1)
            else:
                parts = d.split("/")
                while parts and TEST_DIR_RE.match(parts[-1]):
                    parts.pop()
                mod = "/".join(parts)
            self.under[mod].add(d)

    def module_of(self, f):
        d = os.path.dirname(f)
        while d and not os.path.exists(os.path.join(self.root, d, "ya.make")):
            d = os.path.dirname(d)
        return d

    def test_of(self, f):
        d = os.path.dirname(f)
        while d and d not in self.tests:
            d = os.path.dirname(d)
        return d

    def candidates(self, f):
        d = self.module_of(f)
        while d and d not in self.under:
            d = os.path.dirname(d)
        return self.under[d] if d else set()

    def select(self, files):
        """{"tests": [dir], "modules": [dir], "how": {rule: files}}"""
        files = [f for f in files if not (f.endswith(".md") or f.startswith((".github/", "ydb/docs/")))]
        own, rest = set(), []
        for f in files:
            d = self.test_of(f)
            if d:
                own.add(d)
            else:
                rest.append(f)
        tests, modules = set(own), set()
        how = collections.Counter({"own test": len(own)} if own else {})
        for f in rest:
            cand = self.candidates(f)
            if cand & own:
                how["covered by own"] += 1
                continue
            if cand and len(cand) <= CAP:
                tests |= cand
                how["module"] += 1
                continue
            if cand:
                tk = tokens(os.path.splitext(os.path.basename(f))[0])
                named = {c for c in cand if tk & tokens(c[len(os.path.commonpath([c, f])):])}
                if 0 < len(named) <= CAP:
                    tests |= named
                    how["by name"] += 1
                    continue
            mod = self.module_of(f) if f.endswith(CXX) else ""
            if mod and os.path.exists(os.path.join(self.root, mod, "BUILD.gn")):
                modules.add(mod)
                how["own module"] += 1
            else:
                how["broad" if cand else "none"] += 1
        modules = sorted(modules)[:CAP] if not tests else []
        return {"tests": sorted(tests), "modules": modules, "how": dict(how)}


def changed_files(root, commit):
    out = subprocess.run(["git", "-C", root, "diff", "--name-only", "--no-renames", commit + "^1", commit],
                         stdout=subprocess.PIPE, universal_newlines=True, check=True).stdout
    return out.split()


def select(root, commit):
    return Selector(root).select(changed_files(root, commit))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default=".")
    ap.add_argument("--range", help="first-parent commits A..B")
    ap.add_argument("--json", help="with --range: the selection per commit")
    ap.add_argument("commits", nargs="*")
    args = ap.parse_args()
    root = os.path.realpath(args.root)
    sel = Selector(root)
    if not args.range:
        for c in args.commits:
            print(json.dumps(dict(sel.select(changed_files(root, c)), commit=c)))
        return
    revs = subprocess.run(["git", "-C", root, "rev-list", "--first-parent", "--reverse", args.range],
                          stdout=subprocess.PIPE, universal_newlines=True, check=True).stdout.split()
    out = []
    for u in revs:
        files = changed_files(root, u)
        r = sel.select(files)
        subj = subprocess.run(["git", "-C", root, "log", "-1", "--format=%s", u], stdout=subprocess.PIPE,
                              universal_newlines=True).stdout.strip()
        out.append(dict(r, commit=u[:11], files=len(files), subject=subj[:60]))
    if args.json:
        with open(args.json, "w") as f:
            json.dump(out, f, indent=1)
    n = collections.Counter()
    for r in out:
        k = len(r["tests"])
        n["modules only" if not k and r["modules"] else "0" if not k else "1" if k == 1 else "2-3" if k <= 3
          else "4-10" if k <= 10 else "11-30" if k <= 30 else ">30"] += 1
    print("%d commits; tests per commit: %s" % (len(out), dict(sorted(n.items()))))


if __name__ == "__main__":
    main()
