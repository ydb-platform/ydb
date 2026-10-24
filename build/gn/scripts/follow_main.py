#!/usr/bin/env python3
"""Moves the GN branch along the upstream main one commit at a time (the front
pipeline of the GN follow-up): for every first-parent commit U of the upstream
after the last one merged

  1. merge     git merge U (rerere on); a conflict left -> STOP conflict
  2. review    U touches what the generator does not carry over: contrib
               (ya.make, *.inc, added/deleted sources), a module of a
               hand-written BUILD.gn (`yamake2gn: keep`), the ya build
               configuration (build/conf, ymake.core.conf, ...) -> STOP review
  3. regen     yamake2gn.py; a new item in the `skipped` or an ERROR section of
               its report (against the last accepted one) -> STOP generator
  4. gn gen    failed -> STOP gn-gen
  5. build     autofix.py: ninja, uncommenting the edges it can; still
               failing -> STOP build
  6. commit    `gn: sync <U>` with what 3-5 changed (if anything)
  7. bench     with --bench-root: follow_bench.py measures the incremental
               build of the commit by ninja and by ya make in the bench work
               tree, one after another (the order alternates): ninja --targets
               (all tests), ya --ya-dirs (ydbd, ydb) and what follow_select.py
               picks for U, after ya builds them in the previous commit
               (follow_bench.py --warm); a failed build
               -> STOP bench (a finding: not fixed there; --ack skips the bench
               of this step, running again measures again)
  8. amend     the GN-Bench-* trailers of the measurements go into the message
               of the commit (the tree stays the same)

A commit that touches only docs and .github skips 3-5. The branch itself is the
state: the last merged U is the merge-base of HEAD and the upstream, the
journal tells whether its step is done. After a STOP fix the work tree (or
finish the merge) and run again: the step resumes after the merge; --ack
accepts what the step stopped at (the review, the generator report, the
starting tree, the bench) for the rest of the step.

The first run checks the starting point: the generator on it must change
nothing (else STOP idempotence; --ack commits what it changed) and the build
must pass; with --bench-root it then builds the starting point in the bench
from scratch (warming the caches of the bench).

The front build and the bench must not share a compiler cache: give the bench
a tag that is isolated in the config of dist-clangd (--bench-env
DC_TAGS=gn/follow/bench; with `distclang = true` in args.gn the builds are
tagged with the branch too). The build dirs may be inside the work trees
(e.g. <work tree>/out/gn): the generator and autofix.py skip them. autofix.py and follow_bench.py are taken
from the dir of this script, yamake2gn.py from the work tree.

    follow_main.py --out <build dir> --state <dir> [--root <work tree>]
                   [--upstream origin/main] [--fetch] [--max-steps N] [--ack]
                   [--env NAME=VALUE ...] [-j N] [--push REMOTE]
                   [--bench-root <work tree> --bench-out <build dir> --ya-out <dir>
                    [--ya-cache-root DIR] [--ya-cache-size 120GiB] [--ya-dirs D ...] [--no-select] [--bench-env NAME=VALUE ...] [--bench-targets T ...]]
    follow_main.py --state <dir> --classify [--root ...]   # what is pending, changes nothing

State dir: journal.jsonl (a record per step and per --ack), report.txt (the
accepted generator report), blocked.txt (edges gn gen rejected, for
autofix.py), STOPPED.md (why the last run stopped), steps/<U>/ (logs of a
step, bench.json).

Exit code: 0 nothing left (or --max-steps done), 1 usage/precondition,
10 review, 11 conflict, 12 generator, 13 gn gen, 14 build, 15 idempotence,
16 bench.
"""

import argparse
import fcntl
import json
import os
import re
import shlex
import subprocess
import sys
import time

SCRIPTS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPTS)
import follow_select  # noqa: E402

EXIT = {"review": 10, "conflict": 11, "generator": 12, "gn-gen": 13, "build": 14, "idempotence": 15,
        "bench": 16}
ACKABLE = ("review", "generator", "idempotence", "bench")

DEFAULT_GEN_ARGS = "--as-target --recurse --prune-root-groups ydb"
DEFAULT_TARGETS = ["all", "tests"]
DEFAULT_YA_DIRS = ["ydb/apps/ydbd", "ydb/apps/ydb"]

# what the hand-written BUILD.gn of contrib and of `keep` modules list
SOURCE_EXTS = (".c", ".cc", ".cpp", ".cxx", ".S", ".s", ".asm", ".rl6", ".rl", ".y", ".l",
               ".proto", ".fbs", ".gperf", ".ev", ".sfdl", ".pyx")
# the ya build configuration GN mirrors by hand
YA_CONFIG = ("build/ymake.core.conf", "build/ymake_conf.py", "build/conf/", "build/plugins/",
             "build/platform/", "build/external_resources/", "build/prebuilt/")
# ...but not the parts of other languages, GN builds C++ only
YA_CONFIG_OTHER = ("build/conf/ts/", "build/conf/java", "build/conf/go", "build/platform/java/",
                   "build/platform/go/", "build/platform/yfm/", "build/plugins/lib/nots/",
                   "build/plugins/nots.py", "build/plugins/java.py", "build/plugins/go.py",
                   "build/external_resources/pnpm/")
# changes worth a note in the journal (the bench builds with ya too)
INFO = (("ya", "ya"), ("ya.conf", "ya"), ("build/scripts/", "build scripts"))
# a commit touching only these builds nothing
NO_BUILD = (".github/", "ydb/docs/")


class Stop(Exception):
    def __init__(self, kind, text):
        super().__init__(text)
        self.kind, self.text = kind, text


# ---- git ----------------------------------------------------------------------

class Git:
    def __init__(self, root):
        self.root = root

    def run(self, *args, check=True, env=None):
        r = subprocess.run(["git", "-C", self.root] + list(args), stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE, universal_newlines=True, env=env)
        if check and r.returncode != 0:
            raise RuntimeError("git %s: %s" % (" ".join(args), r.stderr.strip()))
        return r

    def out(self, *args):
        return self.run(*args).stdout.strip()

    def lines(self, *args):
        return [l for l in self.out(*args).splitlines() if l]

    def merge_head(self):
        path = self.out("rev-parse", "--git-path", "MERGE_HEAD")
        path = os.path.join(self.root, path) if not os.path.isabs(path) else path
        if os.path.exists(path):
            with open(path) as f:
                return f.read().split()[0]
        return None

    def dirty(self):
        return self.lines("status", "--porcelain")

    def unmerged(self):
        return self.lines("diff", "--name-only", "--diff-filter=U")

    def subject(self, rev):
        return self.out("log", "-1", "--format=%s", rev)

    def short(self, rev):
        return self.out("rev-parse", "--short=11", rev)


# ---- journal ------------------------------------------------------------------

class State:
    def __init__(self, path):
        self.path = path
        os.makedirs(os.path.join(path, "steps"), exist_ok=True)
        self.journal = os.path.join(path, "journal.jsonl")
        self.report = os.path.join(path, "report.txt")
        self.blocked = os.path.join(path, "blocked.txt")
        self.stopped = os.path.join(path, "STOPPED.md")

    def records(self):
        if not os.path.exists(self.journal):
            return []
        with open(self.journal) as f:
            return [json.loads(l) for l in f if l.strip()]

    def append(self, rec):
        rec = dict(rec, time=time.strftime("%Y-%m-%dT%H:%M:%S"))
        with open(self.journal, "a") as f:
            f.write(json.dumps(rec, sort_keys=True) + "\n")

    def done(self):
        """The upstream commits whose steps are done (the bootstrap's one too)."""
        return {r["upstream"] for r in self.records() if r["status"] in ("done", "bootstrap")}

    def bootstrapped(self):
        return any(r["status"] == "bootstrap" for r in self.records())

    def last_stop(self):
        recs = [r for r in self.records() if r["status"] != "ack"]
        return recs[-1] if recs and recs[-1]["status"] == "stopped" else None

    def acked(self, u):
        """The stages --ack accepted for the step of u."""
        return {r["stage"] for r in self.records() if r["status"] == "ack" and r["upstream"] == u}

    def stopped_at(self, u):
        """The stages the step of u stopped at so far."""
        return {r["stage"] for r in self.records() if r["status"] == "stopped" and r["upstream"] == u}

    def step_dir(self, name):
        d = os.path.join(self.path, "steps", name)
        os.makedirs(d, exist_ok=True)
        return d


# ---- classification -----------------------------------------------------------

def changes(git, u):
    """[(status letter, path)] of U against its first parent."""
    out = []
    for line in git.lines("diff", "--name-status", "--no-renames", u + "^1", u):
        status, path = line.split("\t", 1)
        out.append((status[0], path))
    return out


def keep_dirs(git):
    return {os.path.dirname(p) for p in
            git.run("grep", "-l", "-e", "yamake2gn: keep", "--", "*BUILD.gn", check=False).stdout.split()}


def is_listed(status, path):
    """Does the change show in a hand-written BUILD.gn listing the sources?"""
    name = os.path.basename(path)
    return name == "ya.make" or name.endswith(".inc") or (status in "AD" and name.endswith(SOURCE_EXTS))


def keep_owner(root, keeps, path):
    """The `keep` module dir the path belongs to, or None."""
    x = os.path.dirname(path)
    while x:
        if x in keeps:
            return x
        if os.path.exists(os.path.join(root, x, "ya.make")):
            return None
        x = os.path.dirname(x)
    return None


def classify(root, git, u, keeps):
    """{"review": [(why, path)], "info": [(what, path)], "files": N, "build": bool}"""
    review, info, build = [], [], False
    ch = changes(git, u)
    for status, path in ch:
        if not path.startswith(NO_BUILD) and not path.endswith(".md"):
            build = True
        if path.startswith("contrib/"):
            if is_listed(status, path):
                review.append(("contrib", "%s %s" % (status, path)))
        elif path.startswith(YA_CONFIG):
            if not path.startswith(YA_CONFIG_OTHER):
                review.append(("ya config", "%s %s" % (status, path)))
        elif is_listed(status, path):
            owner = keep_owner(root, keeps, path)
            if owner is not None:
                review.append(("keep //%s" % owner, "%s %s" % (status, path)))
        for prefix, what in INFO:
            if path == prefix or (prefix.endswith("/") and path.startswith(prefix)):
                info.append((what, "%s %s" % (status, path)))
    return {"review": review, "info": info, "files": len(ch), "build": build}


# ---- generator report -----------------------------------------------------------

def report_problems(text):
    """{heading: {item}} of the sections of a yamake2gn report that need a fix:
    `skipped (non-trivial)` and the ERROR ones."""
    start = text.rfind("==================== yamake2gn report")
    sections, cur = {}, None
    for line in text[max(start, 0):].splitlines()[1:]:
        if not line.strip():
            cur = None
        elif not line.startswith(" "):
            cur = line[:-1] if line.endswith(":") and line.startswith(("ERROR", "skipped")) else None
            if cur:
                sections.setdefault(cur, set())
        elif cur:
            sections[cur].add(" ".join(line.split()))
    return sections


def new_problems(old_text, new_text):
    old = report_problems(old_text)
    new = {}
    for heading, items in report_problems(new_text).items():
        added = items - old.get(heading, set())
        if added:
            new[heading] = sorted(added)
    return new


# ---- the step -----------------------------------------------------------------

class Follower:
    def __init__(self, args):
        self.args = args
        self.root = os.path.realpath(args.root)
        self.out = os.path.realpath(args.out) if args.out else None
        self.git = Git(self.root)
        self.state = State(os.path.realpath(args.state))
        self.env = dict(os.environ)
        for kv in args.env:
            name, _, value = kv.partition("=")
            self.env[name] = value
        self.acked = set()

    def say(self, text):
        print("%s %s" % (time.strftime("%H:%M:%S"), text), flush=True)

    def sh(self, cmd, log, cwd=None):
        """Runs cmd with its output into the log; (rc, seconds)."""
        t = time.time()
        with open(log, "w") as f:
            rc = subprocess.run(cmd, cwd=cwd or self.root, stdout=f, stderr=subprocess.STDOUT,
                                env=self.env).returncode
        return rc, round(time.time() - t, 1)

    # -- stages

    def merge(self, u, rec):
        t = time.time()
        r = self.git.run("-c", "rerere.enabled=true", "-c", "rerere.autoUpdate=true", "merge", "--no-ff",
                         "--no-edit", "-m", "Merge %s: %s" % (self.git.short(u), self.git.subject(u)), u,
                         check=False)
        if r.returncode != 0:
            if not self.git.merge_head():
                raise Stop("conflict", "git merge %s failed:\n%s" % (u, (r.stdout + r.stderr).strip()))
            self.finish_merge(u)
        rec["seconds"]["merge"] = round(time.time() - t, 1)

    def finish_merge(self, u):
        left = self.git.unmerged()
        if left:
            raise Stop("conflict", "merge of %s left conflicts (rerere did not resolve them):\n%s\n\n"
                       "Resolve them and commit the merge (or leave it resolved and uncommitted), then run again."
                       % (u, "\n".join("  " + p for p in left)))
        self.git.out("commit", "--no-edit")

    def review(self, u, cls):
        if cls["review"] and "review" not in self.acked:
            raise Stop("review", "%s touches what the generator does not carry over:\n%s\n\n"
                       "Mirror it by hand in the work tree (uncommitted), then run again with --ack."
                       % (u, "\n".join("  %-30s %s" % w for w in cls["review"])))

    def regen(self, step, rec, idempotence):
        py = self.args.python
        cmd = [py, os.path.join(self.root, "build/gn/scripts/yamake2gn.py"), "--root", self.root,
               "--build-dir", self.out] + shlex.split(self.args.gen_args)
        before = set(self.git.dirty())
        t = time.time()
        with open(os.path.join(step, "yamake2gn.out"), "w") as o, \
                open(os.path.join(step, "yamake2gn.txt"), "w") as e:
            rc = subprocess.run(cmd, cwd=self.root, stdout=o, stderr=e, env=self.env).returncode
        rec["seconds"]["regen"] = round(time.time() - t, 1)
        with open(os.path.join(step, "yamake2gn.txt")) as f:
            report = f.read()
        m = re.search(r"generated : (\d+)\s+deleted\(BUILD.gn\): (\d+)", report)
        if m:
            rec["generated"], rec["deleted"] = int(m.group(1)), int(m.group(2))
        if rc != 0:
            raise Stop("generator", "yamake2gn.py failed (rc %d), see %s/yamake2gn.txt" % (rc, step))
        changed = sorted(set(self.git.dirty()) - before)
        rec["regen_changed"] = len(changed)
        if idempotence and changed:
            raise Stop("idempotence", "the generator changed the starting tree (%d files):\n%s\n\n"
                       "Run again with --ack to commit it as the starting point, or fix the generator."
                       % (len(changed), "\n".join("  " + c for c in changed[:50])))
        old = ""
        if os.path.exists(self.state.report):
            with open(self.state.report) as f:
                old = f.read()
        new = new_problems(old, report) if old else {}
        if new and "generator" not in self.acked:
            raise Stop("generator", "new problems in the generator report (%s/yamake2gn.txt):\n%s\n\n"
                       "Fix them (ya.make, a keep BUILD.gn, ...) and run again, or --ack to accept them."
                       % (step, "\n".join("%s:\n%s" % (h, "\n".join("  " + i for i in items))
                                          for h, items in new.items())))
        rec["report"] = report

    def gn_gen(self, step, rec):
        rc, s = self.sh(["gn", "gen", "--script-executable=" + self.args.script_executable, self.out],
                        os.path.join(step, "gn_gen.log"))
        rec["seconds"]["gn_gen"] = s
        if rc != 0:
            with open(os.path.join(step, "gn_gen.log")) as f:
                text = f.read()
            raise Stop("gn-gen", "gn gen failed:\n%s" % text[-4000:])

    def build(self, step, rec):
        summary = os.path.join(step, "autofix.json")
        cmd = [self.args.python, os.path.join(SCRIPTS, "autofix.py"),
               "--root", self.root, "--out", self.out, "--log-dir", os.path.join(step, "autofix"),
               "--blocked", self.state.blocked, "--summary", summary] \
            + (["-j", str(self.args.jobs)] if self.args.jobs else []) + self.args.targets
        self.say("  build: autofix.py %s" % " ".join(self.args.targets))
        rc, s = self.sh(cmd, os.path.join(step, "autofix.log"))
        rec["seconds"]["build"] = s
        result = {}
        if os.path.exists(summary):
            with open(summary) as f:
                result = json.load(f)
        rec["build"] = {k: result.get(k) for k in ("rc", "runs", "failed")}
        rec["build"]["fixed"] = len(result.get("fixed", []))
        if rc != 0:
            raise Stop("build", "the build fails after %s run(s) of autofix.py (%d edges uncommented), "
                       "%s failed commands; unresolved:\n%s\n\nLogs: %s/autofix/. Fix the work tree "
                       "(uncommitted) and run again." % (
                           result.get("runs"), len(result.get("fixed", [])), result.get("failed"),
                           "\n".join("  " + u for u in result.get("unresolved", [])[:60]) or "  (none)",
                           step))

    def commit(self, message, rec):
        self.git.out("add", "-A")
        if self.git.run("diff", "--cached", "--quiet", check=False).returncode != 0:
            rec["sync_files"] = len(self.git.lines("diff", "--cached", "--name-only"))
            self.git.out("commit", "-q", "-m", message)
        rec["result"] = self.git.out("rev-parse", "HEAD")

    def accept_report(self, rec):
        if "report" in rec:
            with open(self.state.report, "w") as f:
                f.write(rec.pop("report"))

    def bench(self, step, rec, full):
        """Measures HEAD in the bench (follow_bench.py); the trailers for its message."""
        if not self.args.bench_root:
            return []
        if "bench" in self.acked:
            rec["bench"] = "skipped"
            return []
        if self.git.dirty():
            raise Stop("bench", "the work tree has changes, the commit to measure is HEAD:\n%s"
                       % "\n".join("  " + l for l in self.git.dirty()[:30]))
        benched = sum(1 for r in self.state.records() if isinstance(r.get("bench"), dict))
        result = os.path.join(step, "bench.json")
        a = self.args
        cmd = [a.python, os.path.join(SCRIPTS, "follow_bench.py"), "--root", a.bench_root,
               "--out", a.bench_out, "--ya-out", a.ya_out, "--commit", self.git.out("rev-parse", "HEAD"),
               "--result", result, "--order", "ninja,ya" if benched % 2 == 0 else "ya,ninja",
               "--python", a.python, "--script-executable", a.script_executable,
               "--targets"] + (a.bench_targets or a.targets)
        cmd += ["--env=" + kv for kv in a.bench_env]
        cmd += (["--ya-cache-size", a.ya_cache_size] if a.ya_cache_size else [])
        dirs = list(a.ya_dirs)
        if not full and not a.no_select:
            sel = follow_select.select(self.root, rec["upstream"])
            rec["select"] = sel
            dirs += [d for d in sel["tests"] + sel["modules"] if d not in dirs]
            self.say("  select: %s" % (" ".join(sel["tests"] + sel["modules"]) or "-"))
        cmd += ["--ya-dirs"] + dirs + ([] if full else ["--warm"])
        cmd += (["--ya-cache-root", a.ya_cache_root] if a.ya_cache_root else []) + (["--full"] if full else [])
        self.say("  bench%s" % (" (full)" if full else ""))
        rc, s = self.sh(cmd, os.path.join(step, "bench.log"))
        rec["seconds"]["bench"] = s
        data = {}
        if os.path.exists(result):
            with open(result) as f:
                data = json.load(f)
        rec["bench"] = {side: {k: v for k, v in data[side].items() if k not in ("executed_seconds", "evlog")}
                        for side in ("ninja", "ya") if side in data}
        if data.get("ya_dirs"):
            rec["bench"]["ya_targets"] = {k: data.get(k) for k in ("ya_dirs", "ya_missing", "ya_warm")}
        if rc != 0:
            raise Stop("bench", "the bench build of %s failed (rc %d): %s\nLogs: %s/bench*.log. A finding of "
                       "the incremental build: look into it, do not fix it in the bench tree; run again to "
                       "measure again, or --ack to go on without the measurement."
                       % (self.git.short("HEAD"), rc, ", ".join(
                           "%s: %s" % (k, v.get("failed")) for k, v in rec["bench"].items() if v.get("failed"))
                          or "see bench.log", step))
        return data["trailers"]

    def amend(self, trailers, rec):
        if trailers:
            msg = self.git.out("log", "-1", "--format=%B", "HEAD")
            self.git.out("commit", "-q", "--amend", "-m", msg.rstrip() + "\n\n" + "\n".join(trailers))
        rec["result"] = self.git.out("rev-parse", "HEAD")

    # -- steps

    def bootstrap(self):
        base = self.git.out("merge-base", "HEAD", self.args.upstream)
        name = "bootstrap-" + self.git.short(base)
        step = self.state.step_dir(name)
        rec = {"upstream": base, "status": "bootstrap", "seconds": {}, "env": self.args.env}
        self.say("bootstrap at %s (upstream %s)" % (self.git.short("HEAD"), self.git.short(base)))
        self.acked = self.state.acked(name)
        stopped = self.state.stopped_at(name)
        # an earlier run stopped past the check: what is in the work tree now is fixes
        check = not (stopped - {"idempotence"}) and "idempotence" not in self.acked

        def run():
            if "bench" not in stopped:
                if check and self.git.dirty():
                    raise Stop("idempotence", "the work tree must be clean to check the generator:\n%s"
                               % "\n".join("  " + l for l in self.git.dirty()[:30]))
                self.regen(step, rec, idempotence=check)
                self.gn_gen(step, rec)
                self.build(step, rec)
                self.commit("gn: regen at %s" % self.git.short(base), rec)
            self.bench(step, rec, full=True)     # no trailers: a build from scratch
            rec["result"] = self.git.out("rev-parse", "HEAD")

        self.guarded(name, rec, run)
        rec["acked"] = sorted(self.acked)
        self.accept_report(rec)
        self.state.append(rec)

    def step(self, u, merged):
        short = self.git.short(u)
        step = self.state.step_dir(short)
        rec = {"upstream": u, "subject": self.git.subject(u), "status": "done", "seconds": {},
               "committed": self.git.out("log", "-1", "--format=%cI", u), "env": self.args.env}
        self.say("%s %s" % (short, rec["subject"][:90]))
        self.acked = self.state.acked(u)
        committed = "bench" in self.state.stopped_at(u)    # it stopped after the commit

        def run():
            if not committed:
                if not merged:
                    self.merge(u, rec)
                cls = classify(self.root, self.git, u, keep_dirs(self.git))
                rec.update(files=cls["files"], review=cls["review"], info=cls["info"])
                with open(os.path.join(step, "classify.json"), "w") as f:
                    json.dump(cls, f, indent=1)
                self.review(u, cls)
                if not cls["build"] and not self.git.dirty():
                    rec["noop"] = True
                else:
                    self.regen(step, rec, idempotence=False)
                    self.gn_gen(step, rec)
                    self.build(step, rec)
                self.commit("gn: sync %s: %s" % (short, rec["subject"]), rec)
            self.amend(self.bench(step, rec, full=False), rec)

        self.guarded(u, rec, run)
        rec["acked"] = sorted(self.acked)
        self.accept_report(rec)
        self.state.append(rec)
        if os.path.exists(self.state.stopped):
            os.remove(self.state.stopped)
        if self.args.push:
            self.git.out("push", "-q", self.args.push, "HEAD:refs/heads/" + self.git.out(
                "rev-parse", "--abbrev-ref", "HEAD"))

    def guarded(self, u, rec, body):
        try:
            body()
        except Stop as s:
            rec.pop("report", None)
            self.record_stop(u, rec, s)
            raise

    def record_stop(self, u, rec, s):
        self.state.append(dict(rec, status="stopped", stage=s.kind, upstream=u))
        with open(self.state.stopped, "w") as f:
            f.write("# STOP %s at %s\n\n%s\n" % (s.kind, u, s.text))

    # -- the loop

    def current(self):
        """(U, merged) of the step to do, or None when the upstream is reached."""
        mh = self.git.merge_head()
        if mh:
            self.finish_merge(mh)
            return mh, True
        last = self.git.out("merge-base", "HEAD", self.args.upstream)
        if last not in self.state.done():
            return last, True
        if self.git.dirty():
            raise Stop("conflict", "the work tree has changes, but the step of %s is done:\n%s"
                       % (last, "\n".join("  " + l for l in self.git.dirty()[:30])))
        pending = self.git.lines("rev-list", "--first-parent", "--reverse", last + ".." + self.args.upstream)
        return (pending[0], False) if pending else None

    def follow(self):
        if self.args.fetch:
            remote, _, branch = self.args.upstream.partition("/")
            self.git.out("fetch", "-q", remote, branch)
        if self.args.ack:
            stop = self.state.last_stop()
            if not stop or stop["stage"] not in ACKABLE:
                raise SystemExit("--ack: the last run did not stop at %s" % "/".join(ACKABLE))
            self.state.append({"upstream": stop["upstream"], "status": "ack", "stage": stop["stage"]})
        if not self.state.bootstrapped():
            self.bootstrap()
        steps = 0
        while self.args.max_steps is None or steps < self.args.max_steps:
            try:
                cur = self.current()
            except Stop as s:
                self.record_stop(self.git.merge_head() or "HEAD", {"seconds": {}}, s)
                raise
            if cur is None:
                self.say("the upstream is reached")
                break
            self.step(*cur)
            steps += 1
        return 0


def classify_only(args):
    root = os.path.realpath(args.root)
    git = Git(root)
    last = git.out("merge-base", "HEAD", args.upstream)
    pending = git.lines("rev-list", "--first-parent", "--reverse", last + ".." + args.upstream)
    keeps = keep_dirs(git)
    totals = {"review": 0, "noop": 0}
    for u in pending[:args.max_steps] if args.max_steps else pending:
        c = classify(root, git, u, keeps)
        mark = "REVIEW" if c["review"] else ("noop" if not c["build"] else "")
        totals["review"] += bool(c["review"])
        totals["noop"] += not c["build"]
        print("%s %-6s %4d files  %s" % (git.short(u), mark, c["files"], git.subject(u)[:80]))
        for why, path in c["review"] + c["info"]:
            print("      %-28s %s" % (why, path))
    print("pending: %d, to review: %d, docs only: %d" % (len(pending), totals["review"], totals["noop"]))
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default=os.getcwd(), help="the work tree of the GN branch (default: cwd)")
    ap.add_argument("--out", help="the build dir (args.gn there)")
    ap.add_argument("--state", required=True)
    ap.add_argument("--upstream", default="origin/main")
    ap.add_argument("--fetch", action="store_true", help="git fetch the upstream first")
    ap.add_argument("--max-steps", type=int)
    ap.add_argument("--ack", action="store_true", help="accept the review / generator report of the current step")
    ap.add_argument("--env", action="append", default=[], metavar="NAME=VALUE",
                    help="for gn gen and ninja, e.g. the compiler cache's (repeatable)")
    ap.add_argument("-j", "--jobs", type=int, help="ninja -j")
    ap.add_argument("--targets", nargs="+", default=DEFAULT_TARGETS)
    ap.add_argument("--gen-args", default=DEFAULT_GEN_ARGS, help="of yamake2gn.py")
    ap.add_argument("--python", default=sys.executable, help="for yamake2gn.py and autofix.py")
    ap.add_argument("--script-executable", default=os.environ.get("GN_SCRIPT_EXECUTABLE", "python3.9"),
                    help="gn gen --script-executable")
    ap.add_argument("--push", metavar="REMOTE", help="push the branch after every step")
    ap.add_argument("--bench-root", help="the bench work tree (a worktree of this repo): measure every step")
    ap.add_argument("--bench-out", help="the bench build dir (args.gn there)")
    ap.add_argument("--ya-out", help="ya make -o of the bench")
    ap.add_argument("--ya-cache-root", help="YA_CACHE_DIR of the bench (default: ~/.ya)")
    ap.add_argument("--ya-cache-size", help="ya make --cache-size of the bench, e.g. 120GiB")
    ap.add_argument("--ya-dirs", nargs="+", default=DEFAULT_YA_DIRS,
                    help="ya make of the bench, besides the targets follow_select.py picks for the step")
    ap.add_argument("--no-select", action="store_true", help="ya builds only --ya-dirs")
    ap.add_argument("--bench-env", action="append", default=[], metavar="NAME=VALUE",
                    help="for gn gen and ninja of the bench, e.g. its compiler cache tag (repeatable)")
    ap.add_argument("--bench-targets", nargs="+", help="of the bench, ninja and ya (default: --targets)")
    ap.add_argument("--classify", action="store_true", help="only show the pending commits and their review")
    args = ap.parse_args()

    if args.classify:
        return classify_only(args)
    if not args.out:
        ap.error("--out is required")
    if args.bench_root and not (args.bench_out and args.ya_out):
        ap.error("--bench-root needs --bench-out and --ya-out")
    for path in ("bench_root", "bench_out", "ya_out", "ya_cache_root"):
        if getattr(args, path):
            setattr(args, path, os.path.realpath(getattr(args, path)))
    os.makedirs(args.state, exist_ok=True)
    lock = open(os.path.join(args.state, "lock"), "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        sys.exit("follow_main.py: another run holds %s/lock" % args.state)
    f = Follower(args)
    try:
        return f.follow()
    except Stop as s:
        print("\nSTOP %s\n%s" % (s.kind, s.text), file=sys.stderr)
        return EXIT[s.kind]


if __name__ == "__main__":
    sys.exit(main())
