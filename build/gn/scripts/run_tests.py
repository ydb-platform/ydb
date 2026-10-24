#!/usr/bin/env python3
"""Runs the tests of a GN build the way the ya CI does (`ya make -A`), as far as
GN knows how: the C++ tests (UNITTEST, UNITTEST_FOR, GTEST) of //:tests.

    run_tests.py --build-dir <out> [--test-size small,medium] [--test-threads N]
                 [--retries 3] [--muted .github/config/muted_ya.txt] [--results DIR]
                 [--filter SUBSTR] [--launcher CMD] [--list]

--launcher CMD prefixes every run of a test binary, listing included:
`CMD <binary> <args>`. By default it is <build dir>/fastld (//build/gn/fastld,
a loader that starts the binaries of ~1800 .so in ~1 s instead of ~10 s), if
built; --launcher '' runs the binaries directly.

The tests come from the `ya_tests` metadata of //:tests (`gn meta`); what GN does
not keep -- SIZE, TIMEOUT, FORK_TESTS/FORK_SUBTESTS, SPLIT_FACTOR, ENV, TAG,
USE_RECIPE -- is read from the test's ya.make (IF and INCLUDE resolved as by
yamake2gn.py). As in the CI:

  * only the sizes asked for run; ya's default timeout of a size (60/600/3600 s)
    unless TIMEOUT();
  * FORK_SUBTESTS / FORK_TESTS split a test into SPLIT_FACTOR (default 10)
    chunks by test cases / by suites, run in parallel;
  * failed test cases are rerun, up to --retries attempts in all (ya's -X);
  * a failure listed in --muted is MUTE, not FAILED.

Not run (counted apart): TAG(ya:manual), the tests of a recipe (USE_RECIPE: no
runner for recipes in GN yet), the tests whose binary is not built.

Results: <results>/<test>/<chunk>/{out.txt,...}, <results>/summary.json; a
summary to stdout. Exit code 0 unless --strict and a test FAILED.
"""

import argparse
import concurrent.futures
import fnmatch
import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import yamake2gn as Y   # noqa: E402  (ya.make parsing)

# ya's default timeouts of the test sizes, seconds
SIZE_TIMEOUTS = {"SMALL": 60, "MEDIUM": 600, "LARGE": 3600}
DEFAULT_SPLIT_FACTOR = 10
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
UT_FAIL_RE = re.compile(r"^\[FAIL\] (\S+?::\S+?) -> ", re.M)
UT_DONE_RE = re.compile(r"^\[DONE\] ok: (\d+)(?:, err: (\d+))?", re.M)
GT_FAIL_RE = re.compile(r"^\[  FAILED  \] ([^\s,]+\.[^\s,]+)", re.M)
GT_PASS_RE = re.compile(r"^\[  PASSED  \] (\d+) test", re.M)


# --- the tests ------------------------------------------------------------

def gn_meta(root, build_dir, script_executable):
    """[{label, kind, binary, ...}] of the ya_tests metadata of //:tests."""
    out = subprocess.run(["gn", "meta", "--script-executable=" + script_executable,
                          build_dir, "//:tests", "--data=ya_tests"],
                         check=True, stdout=subprocess.PIPE, text=True, cwd=root).stdout
    tests, cur = [], None
    for line in out.splitlines():
        line = line.strip()
        if line == "{":
            cur = {}
        elif line == "}":
            if cur:
                tests.append(cur)
            cur = None
        elif cur is not None:
            m = re.match(r'(\w+) = "(.*)"$', line)
            if m:
                cur[m.group(1)] = m.group(2)
    return tests


def yamake_props(root, d):
    """SIZE, TIMEOUT, fork mode, SPLIT_FACTOR, ENV, TAGs, USE_RECIPE of the test
    module in dir d, as its ya.make says for the configuration GN builds."""
    path = os.path.join(root, d, "ya.make")
    props = {"size": "SMALL", "timeout": None, "fork": None, "split": None,
             "env": {}, "tags": [], "recipes": []}
    try:
        text = open(path, encoding="utf-8").read()
    except OSError:
        return props
    text = Y.strip_yamake_comments(text)
    text = Y.resolve_conditionals(text)
    text = Y.resolve_includes(text, root, d)
    for name, args, raw in Y.scan_macros(text):
        if name == "SIZE" and args:
            props["size"] = args[0].upper()
        elif name == "TIMEOUT" and args and args[0].isdigit():
            props["timeout"] = int(args[0])
        elif name in ("FORK_SUBTESTS", "FORK_TESTS"):
            props["fork"] = name
        elif name == "SPLIT_FACTOR" and args and args[0].isdigit():
            props["split"] = int(args[0])
        elif name == "ENV":
            for a in shlex.split(raw):
                k, _, v = a.partition("=")
                if k:
                    props["env"][k] = v
        elif name == "TAG":
            props["tags"] += args
        elif name == "USE_RECIPE" and args:
            props["recipes"].append(" ".join(args))
    return props


# --- running ------------------------------------------------------------------

class Task:
    """A run of a test binary: the whole test or one of its chunks, with a
    filter of test cases (unittest: "Suite::Test"; gtest: "Suite.Test")."""

    def __init__(self, test, chunk, cases=None, shard=None):
        self.test, self.chunk, self.cases, self.shard = test, chunk, cases, shard
        self.attempts = []      # [{rc, seconds, timed_out, ok, failed: [...], crashed}]


# --- the processes: nothing a test started outlives it, or the runner -----------
#
# A test runs in a process group of its own; when it ends, whatever is left in the
# group (a server it started) is killed. The runner is a child subreaper
# (PR_SET_CHILD_SUBREAPER), as ya's --cleanup-child-processes: a daemon that left
# its group (setsid) is reparented to it and killed when the runner ends. Ctrl+C or
# SIGTERM stop the run: the running tests are killed, the rest not started.

STOPPING = threading.Event()
_RUNNING = {}       # pgid -> Popen
_RUNNING_LOCK = threading.Lock()


def _killpg(pgid):
    try:
        os.killpg(pgid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass


def become_subreaper():
    try:
        import ctypes
        libc = ctypes.CDLL(None, use_errno=True)
        libc.prctl(36, 1, 0, 0, 0)     # PR_SET_CHILD_SUBREAPER
    except (OSError, AttributeError):
        pass


def kill_orphans():
    """SIGKILL the processes reparented to the runner (and their groups)."""
    me = os.getpid()
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            with open("/proc/%s/stat" % pid) as f:
                fields = f.read().rsplit(")", 1)[1].split()
        except OSError:
            continue
        if int(fields[1]) == me and int(pid) not in {p.pid for p in _RUNNING.values()}:
            _killpg(int(fields[2]))
            try:
                os.kill(int(pid), signal.SIGKILL)
            except ProcessLookupError:
                pass


def stop_all(signum=None, frame=None):
    if not STOPPING.is_set():
        print("\n%s stopping: killing the running tests" % time.strftime("%H:%M:%S"),
              file=sys.stderr, flush=True)
    STOPPING.set()
    with _RUNNING_LOCK:
        for pgid in list(_RUNNING):
            _killpg(pgid)
    kill_orphans()


def run_process(argv, cwd, env, timeout, out_path):
    start = time.monotonic()
    with open(out_path, "wb") as out:
        with _RUNNING_LOCK:
            if STOPPING.is_set():
                return -1, 0.0, False
            p = subprocess.Popen(argv, cwd=cwd, env=env, stdout=out, stderr=subprocess.STDOUT,
                                 stdin=subprocess.DEVNULL, start_new_session=True)
            _RUNNING[p.pid] = p
        try:
            rc = p.wait(timeout=timeout)
            timed_out = False
        except subprocess.TimeoutExpired:
            _killpg(p.pid)
            rc = p.wait()
            timed_out = True
        finally:
            _killpg(p.pid)      # what the test left in its group
            with _RUNNING_LOCK:
                _RUNNING.pop(p.pid, None)
    return rc, time.monotonic() - start, timed_out


def parse_output(kind, text):
    """(ok count or None, failed test cases)"""
    text = ANSI_RE.sub("", text)
    if kind == "gtest":
        failed = sorted(set(GT_FAIL_RE.findall(text)))
        m = GT_PASS_RE.search(text)
        return (int(m.group(1)) if m else None), failed
    failed = sorted(set(UT_FAIL_RE.findall(text)))
    m = UT_DONE_RE.search(text)
    return (int(m.group(1)) if m else None), failed


def run_attempt(task, attempt, cases, args):
    test = task.test
    work = os.path.join(args.results, test["name"], "chunk%d" % task.chunk, "try%d" % attempt)
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(os.path.join(work, "testing_out_stuff"))
    env = dict(os.environ)
    env.update(test["props"]["env"])
    env.update({"ARCADIA_SOURCE_ROOT": args.source_root, "ARCADIA_BUILD_ROOT": args.build_dir,
                "TEST_WORK_PATH": work})
    argv = args.launcher + [test["path"]]
    if test["kind"] == "gtest":
        if cases:
            argv.append("--gtest_filter=" + ":".join(cases))
        elif task.shard is not None:
            env["GTEST_TOTAL_SHARDS"], env["GTEST_SHARD_INDEX"] = str(task.shard[1]), str(task.shard[0])
    else:
        argv += ["--print-times", "--show-fails"]
        if cases:
            filt = os.path.join(work, "filter.txt")
            # case names are bytes, not always utf-8: as listed (surrogateescape)
            with open(filt, "w", errors="surrogateescape") as f:
                f.write("".join("+%s\n" % c for c in cases))
            argv += ["--filter-file", filt]
    rc, seconds, timed_out = run_process(argv, work, env, test["timeout"],
                                         os.path.join(work, "out.txt"))
    with open(os.path.join(work, "out.txt"), "rb") as f:
        ok, failed = parse_output(test["kind"], f.read().decode("utf-8", "surrogateescape"))
    crashed = rc != 0 and not failed      # a crash, a timeout or an error before any test
    return {"rc": rc, "seconds": round(seconds, 2), "timed_out": timed_out, "ok": ok,
            "failed": failed, "crashed": crashed, "cases": cases}


class Progress:
    """Running statistics on stderr: a line every --progress seconds, and each
    failure as it happens."""

    def __init__(self, total, interval, t0):
        self.total, self.interval, self.t0 = total, interval, t0
        self.lock = threading.Lock()
        self.done = self.ok = self.failed = self.crashed = self.reruns = 0
        self.running = {}       # id(task) -> (name, started)
        self.stop = threading.Event()

    def log(self, line):
        print("%s %s" % (time.strftime("%H:%M:%S"), line), file=sys.stderr, flush=True)

    def started(self, task, attempt):
        with self.lock:
            name = "%s#%d%s" % (task.test["dir"], task.chunk, " (try %d)" % attempt if attempt > 1 else "")
            self.running[id(task)] = (name, time.monotonic())
            if attempt > 1:
                self.reruns += 1

    def attempt_done(self, task, attempt, res, final):
        with self.lock:
            self.running.pop(id(task), None)
            what = ("timeout" if res["timed_out"] else "crash rc=%d" % res["rc"]) if res["crashed"] else None
            if res["failed"] or what:
                self.log("%s %s#%d try %d: %s" % ("FAIL" if final else "fail", task.test["dir"], task.chunk,
                         attempt, what or ", ".join(res["failed"][:3]) + (" ..." if len(res["failed"]) > 3 else "")))
            if final:
                self.done += 1
                self.ok += res["ok"] or 0
                self.failed += len(res["failed"])
                self.crashed += 1 if res["crashed"] else 0

    def line(self):
        with self.lock:
            el = time.monotonic() - self.t0
            eta = el / self.done * (self.total - self.done) if self.done else 0
            longest = sorted(self.running.values(), key=lambda v: v[1])[:3]
            now = time.monotonic()
            return ("[%d/%d] %s elapsed, ~%s left | running %d | cases ok %d, failed %d; crashed %d, reruns %d"
                    % (self.done, self.total, _hms(el), _hms(eta), len(self.running), self.ok, self.failed,
                       self.crashed, self.reruns)
                    + ("\n           longest: " + "; ".join("%s %s" % (n, _hms(now - st)) for n, st in longest)
                       if longest else ""))

    def run(self):
        while not self.stop.wait(self.interval):
            self.log(self.line())


def _hms(sec):
    sec = int(sec)
    return "%d:%02d:%02d" % (sec // 3600, sec % 3600 // 60, sec % 60)


def run_task(task, args, progress=None):
    cases = task.cases
    for attempt in range(1, args.retries + 1):
        if STOPPING.is_set():
            break
        if progress:
            progress.started(task, attempt)
        res = run_attempt(task, attempt, cases, args)
        if STOPPING.is_set():
            break       # killed by the stop: not a result
        task.attempts.append(res)
        final = (res["rc"] == 0 and not res["failed"]) or attempt == args.retries
        if progress:
            progress.attempt_done(task, attempt, res, final)
        if res["rc"] == 0 and not res["failed"]:
            return task
        if res["crashed"]:
            # nothing to narrow the rerun to: the same cases again
            continue
        cases = res["failed"]
    return task


def unittest_cases(test, args):
    """The test cases of a unittest binary (`--list-verbose`)."""
    env = dict(os.environ, ARCADIA_SOURCE_ROOT=args.source_root, ARCADIA_BUILD_ROOT=args.build_dir)
    env.update(test["props"]["env"])
    try:
        out = subprocess.run(args.launcher + [test["path"], "--list-verbose"], stdout=subprocess.PIPE,
                             stderr=subprocess.DEVNULL, env=env, timeout=120, text=True,
                             errors="surrogateescape").stdout
    except (subprocess.TimeoutExpired, OSError):
        return None
    return [l.strip() for l in out.splitlines() if "::" in l]


def plan_tasks(test, args):
    """Tasks of a test: chunks of FORK_SUBTESTS (by case) / FORK_TESTS (by suite)."""
    props = test["props"]
    if not props["fork"]:
        return [Task(test, 0)]
    n = props["split"] or DEFAULT_SPLIT_FACTOR
    if test["kind"] == "gtest":
        return [Task(test, i, shard=(i, n)) for i in range(n)]
    cases = unittest_cases(test, args)
    if not cases:
        return [Task(test, 0)]
    if props["fork"] == "FORK_TESTS":
        groups = {}
        for c in cases:
            groups.setdefault(c.split("::")[0], []).append(c)
        units = list(groups.values())
    else:
        units = [[c] for c in cases]
    chunks = [[] for _ in range(min(n, len(units)))]
    for i, u in enumerate(units):
        chunks[i % len(chunks)].extend(u)
    return [Task(test, i, cases=c) for i, c in enumerate(chunks)]


# --- muted -------------------------------------------------------------------

def load_muted(path):
    out = []
    if not path or not os.path.exists(path):
        return out
    for line in open(path, encoding="utf-8"):
        parts = line.strip().split(None, 1)
        if len(parts) == 2:
            out.append((parts[0], parts[1]))
    return out


def is_muted(muted, d, case):
    name = case.replace("::", ".")
    return any(fnmatch.fnmatchcase(d, p) and fnmatch.fnmatchcase(name, c) for p, c in muted)


def chunk_muted(muted, d):
    """A crash or a timeout of a whole chunk: muted by a "... chunk" entry."""
    return any(fnmatch.fnmatchcase(d, p) and c.endswith("chunk") for p, c in muted)


# --- main ----------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--build-dir", required=True)
    ap.add_argument("--source-root", default=os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))
    ap.add_argument("--test-size", default="small,medium")
    ap.add_argument("--test-threads", type=int, default=os.cpu_count())
    ap.add_argument("--retries", type=int, default=3, help="attempts in all (the CI's TEST_RETRY_COUNT)")
    ap.add_argument("--muted", default=None, help="default: <source root>/.github/config/muted_ya.txt")
    ap.add_argument("--results", default=None, help="default: <build dir>/test-results")
    ap.add_argument("--filter", action="append", default=[], help="only the tests whose dir contains it")
    ap.add_argument("--script-executable", default="python3.9")
    ap.add_argument("--launcher", default=None,
                    help="command (shell-split) to run each test binary with; "
                         "default: <build dir>/fastld if built, '' for none")
    ap.add_argument("--list", action="store_true", help="print what would run and why, run nothing")
    ap.add_argument("--strict", action="store_true", help="exit 1 if a test FAILED")
    ap.add_argument("--progress", type=float, default=30, help="seconds between the progress lines (stderr)")
    args = ap.parse_args()
    for stream in (sys.stdout, sys.stderr):
        stream.reconfigure(errors="backslashreplace")
    become_subreaper()
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, stop_all)
    args.build_dir = os.path.abspath(args.build_dir)
    args.source_root = os.path.abspath(args.source_root)
    args.results = os.path.abspath(args.results or os.path.join(args.build_dir, "test-results"))
    if args.launcher is None:
        fastld = os.path.join(args.build_dir, "fastld")
        args.launcher = fastld if os.access(fastld, os.X_OK) else ""
    args.launcher = shlex.split(args.launcher)
    if args.launcher and "/" in args.launcher[0]:
        # the tests run in their own work dirs
        args.launcher[0] = os.path.abspath(args.launcher[0])
    launcher = " via " + shlex.join(args.launcher) if args.launcher else ""
    muted = load_muted(args.muted or os.path.join(args.source_root, ".github/config/muted_ya.txt"))
    sizes = {s.strip().upper() for s in args.test_size.split(",") if s.strip()}

    t0 = time.monotonic()
    tests, skipped = [], {}
    for t in gn_meta(args.source_root, args.build_dir, args.script_executable):
        d = t["label"][2:].partition(":")[0]
        if args.filter and not any(f in d for f in args.filter):
            continue
        t["dir"], t["name"] = d, t["binary"]
        t["path"] = os.path.join(args.build_dir, t["binary"])
        t["props"] = props = yamake_props(args.source_root, d)
        t["timeout"] = props["timeout"] or SIZE_TIMEOUTS.get(props["size"], 600)
        why = None
        if props["size"] not in sizes:
            why = "size " + props["size"].lower()
        elif "ya:manual" in props["tags"]:
            why = "ya:manual"
        elif props["recipes"]:
            why = "recipe"
        elif not os.path.exists(t["path"]):
            why = "not built"
        if why:
            skipped.setdefault(why, []).append(d)
        else:
            tests.append(t)

    if args.list:
        for t in tests:
            p = t["props"]
            print("%-70s %-8s %-6s timeout=%-4d %s" % (t["dir"], t["kind"], p["size"].lower(), t["timeout"],
                  "%s x%d" % (p["fork"], p["split"] or DEFAULT_SPLIT_FACTOR) if p["fork"] else ""))
        for why, ds in sorted(skipped.items()):
            print("skipped (%s): %d" % (why, len(ds)))
        if launcher:
            print("launcher: " + shlex.join(args.launcher))
        return 0

    print("%s %d tests to run%s, skipped: %s; splitting into chunks..." % (
        time.strftime("%H:%M:%S"), len(tests), launcher,
        ", ".join("%s %d" % (w, len(ds)) for w, ds in sorted(skipped.items())) or "none"),
        file=sys.stderr, flush=True)
    shutil.rmtree(args.results, ignore_errors=True)
    os.makedirs(args.results)
    with concurrent.futures.ThreadPoolExecutor(args.test_threads) as pool:
        tasks = [x for ts in pool.map(lambda t: plan_tasks(t, args), tests) for x in ts]
        # the longest first: a lone long test at the end would stretch the run
        tasks.sort(key=lambda x: -x.test["timeout"])
        progress = Progress(len(tasks), args.progress, t0)
        progress.log("%d tests in %d processes, %d threads (planned in %s)"
                     % (len(tests), len(tasks), args.test_threads, _hms(time.monotonic() - t0)))
        ticker = threading.Thread(target=progress.run, daemon=True)
        ticker.start()
        tasks = list(pool.map(lambda task: run_task(task, args, progress), tasks))
        progress.stop.set()
        progress.log(progress.line())
    kill_orphans()
    wall = time.monotonic() - t0
    interrupted = STOPPING.is_set()
    if interrupted:
        # the summary of what has finished
        tasks = [t for t in tasks if t.attempts]

    # per test: the final state of each case
    per_test = {}
    for task in tasks:
        r = per_test.setdefault(task.test["dir"], {"kind": task.test["kind"], "size": task.test["props"]["size"],
                                                  "ok": 0, "failed": [], "muted": [], "crashed": [], "timeouts": 0,
                                                  "seconds": 0.0, "retried": 0})
        first, last = task.attempts[0], task.attempts[-1]
        r["ok"] += first["ok"] or 0
        r["seconds"] += sum(a["seconds"] for a in task.attempts)
        r["retried"] += len(task.attempts) - 1
        r["timeouts"] += sum(1 for a in task.attempts if a["timed_out"])
        bad = last["failed"] if not (last["rc"] == 0 and not last["failed"]) else []
        if last["crashed"] and not bad:
            label = "chunk%d%s" % (task.chunk, " (timeout)" if last["timed_out"] else " (rc=%d)" % last["rc"])
            (r["muted"] if chunk_muted(muted, task.test["dir"]) else r["crashed"]).append(label)
        for c in bad:
            (r["muted"] if is_muted(muted, task.test["dir"], c) else r["failed"]).append(c)
        # cases that passed on a rerun count as ok
        if len(task.attempts) > 1 and not bad and not last["crashed"]:
            r["ok"] += len(first["failed"])

    failed_tests = sorted(d for d, r in per_test.items() if r["failed"] or r["crashed"])
    summary = {
        "wall_seconds": round(wall, 1),
        "interrupted": interrupted,
        "launcher": args.launcher,
        "tests_run": len(per_test),
        "tasks": len(tasks),
        "cases_ok": sum(r["ok"] for r in per_test.values()),
        "cases_failed": sum(len(r["failed"]) for r in per_test.values()),
        "chunks_crashed": sum(len(r["crashed"]) for r in per_test.values()),
        "cases_muted": sum(len(r["muted"]) for r in per_test.values()),
        "timeouts": sum(r["timeouts"] for r in per_test.values()),
        "reruns": sum(r["retried"] for r in per_test.values()),
        "tests_failed": failed_tests,
        "skipped": {why: len(ds) for why, ds in skipped.items()},
        "skipped_dirs": skipped,
        "tests": per_test,
    }
    with open(os.path.join(args.results, "summary.json"), "w") as f:
        json.dump(summary, f, indent=1, sort_keys=True)

    print("tests run: %d (%d processes%s), %.0f s%s" % (len(per_test), len(tasks), launcher, wall,
                                                       " -- INTERRUPTED" if interrupted else ""))
    print("test cases: ok %d, FAILED %d, muted %d; crashed chunks %d, timeouts %d, reruns %d"
          % (summary["cases_ok"], summary["cases_failed"], summary["cases_muted"],
             summary["chunks_crashed"], summary["timeouts"], summary["reruns"]))
    print("skipped: " + (", ".join("%s %d" % kv for kv in sorted(summary["skipped"].items())) or "none"))
    if failed_tests:
        print("FAILED tests:")
        for d in failed_tests:
            r = per_test[d]
            print("  %s: %s" % (d, ", ".join((r["failed"] + r["crashed"])[:5])
                                  + (" ..." if len(r["failed"] + r["crashed"]) > 5 else "")))
    slow = sorted(per_test.items(), key=lambda kv: -kv[1]["seconds"])[:10]
    print("slowest (cpu-seconds of all chunks and reruns):")
    for d, r in slow:
        print("  %8.1f s  %s" % (r["seconds"], d))
    print("details: %s" % os.path.join(args.results, "summary.json"))
    if interrupted:
        return 130
    return 1 if args.strict and failed_tests else 0


if __name__ == "__main__":
    sys.exit(main())
