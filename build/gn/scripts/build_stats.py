#!/usr/bin/env python3
"""Statistics of the last gn/ninja build, kept in a local sqlite database.

    ninja -C <out> <targets>; python3 build/gn/scripts/build_stats.py record <out> --status $?

record     the commands of the last ninja run (successful or not): critical path, machine
           usage, why the commands ran, what PCH won or lost, slower commands than the last
           time; everything goes to the database, a summary to stdout, the full report to
           <db dir>/reports/run-<id>.md
show       the report of a recorded run again
pch        PCH candidates: targets without a PCH that would gain from one, and the PCH that
           don't pay off, over the last N recorded runs (or all of them), weighted by how
           often their TUs were really rebuilt
cost       what touching files would rebuild, and its CPU time by the last known durations
ya         the result of a ya make build of the same main commit (for `series`)
series     the recorded runs by main commit, next to ya make: the Full Test report
baseline   per-TU durations and dependencies of a build without PCH (another build dir),
           the reference of the PCH balance

Data: <out>/.ninja_log (wall time of every command), <out>/.ninja_deps (dependencies),
the ninja manifest (graph, gn target of every command), clang -ftime-trace files of the TUs
(with `time_trace = true` in args.gn: header parse times, for `pch`).

The last run is the entries of .ninja_log the previous `record` didn't see (ninja appends;
when the log is recompacted, an entry is recognized by its times and command hash). ninja
doesn't log failed commands: pass its exit code (--status) and the targets (--targets) to
get what is left.

The database is <source root>/.gn_build_stats/stats.sqlite by default (in .gitignore).
"""

import argparse
import array
import collections
import datetime
import multiprocessing
import os
import re
import sqlite3
import subprocess
import sys
import zlib

import build_time_report as btr

DB_DIR = ".gn_build_stats"
COMPILE_RULES = ("cxx", "cc", "objc", "objcxx", "asm")
LINK_RULES = ("solink", "link", "alink", "solink_module")
MIN_HEADER_US = 1000          # header parse events kept for `pch` (time traces are at 500 us)
REGRESSION_RATIO = 1.5
REGRESSION_MIN_MS = 5000

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY,
    recorded_at TEXT,
    build_dir TEXT,
    started_at REAL,           -- unix time, estimated from the output mtimes
    wall_ms INTEGER,
    cpu_ms INTEGER,
    commands INTEGER,
    status INTEGER,            -- ninja exit code, NULL if not given
    remaining INTEGER,         -- commands left for --targets, NULL if not checked
    remaining_cpu_ms INTEGER,
    git_head TEXT,
    git_dirty INTEGER,
    main_commit TEXT,          -- merge-base of HEAD and the main ref
    main_date TEXT,
    main_subject TEXT,
    label TEXT,
    load_factor REAL,          -- this run's TU durations / their previous ones (without PCH)
    critical_ms INTEGER,       -- the chain of commands that ended the build
    ideal_ms INTEGER,          -- the longest chain with unlimited parallelism
    max_parallel INTEGER,
    low_parallel_ms INTEGER,   -- time with at most a quarter of max_parallel commands
    pch_build_ms INTEGER,
    pch_tus INTEGER,
    pch_saved_ms INTEGER,      -- with PCH vs without, the PCH builds and the extra rebuilds included
    pch_extra_ms INTEGER,      -- TUs rebuilt only because of a PCH header
    pch_estimated INTEGER,     -- PCH TUs without a known duration without PCH (the median ratio)
    pch_unknown INTEGER        -- ... nor a ratio to estimate it: left out of pch_saved_ms
);
CREATE TABLE IF NOT EXISTS names (id INTEGER PRIMARY KEY, name TEXT UNIQUE);
CREATE TABLE IF NOT EXISTS commands (
    run INTEGER, output TEXT, target TEXT, kind TEXT, start_ms INTEGER, end_ms INTEGER,
    cmdhash TEXT, pch INTEGER, reason TEXT);
CREATE INDEX IF NOT EXISTS commands_run ON commands(run);
CREATE INDEX IF NOT EXISTS commands_output ON commands(output);
CREATE TABLE IF NOT EXISTS causes (run INTEGER, name INTEGER, direct INTEGER, commands INTEGER, cpu_ms INTEGER);
CREATE INDEX IF NOT EXISTS causes_run ON causes(run);
CREATE TABLE IF NOT EXISTS pch_targets (
    run INTEGER, target TEXT, tus INTEGER, build_ms INTEGER, extra_ms INTEGER, saved_ms INTEGER);
CREATE TABLE IF NOT EXISTS critical (run INTEGER, pos INTEGER, output TEXT, start_ms INTEGER, end_ms INTEGER);
CREATE TABLE IF NOT EXISTS log_state (
    build_dir TEXT, output TEXT, start_ms INTEGER, end_ms INTEGER, mtime INTEGER, cmdhash TEXT,
    PRIMARY KEY (build_dir, output));
CREATE TABLE IF NOT EXISTS nopch (          -- the last compile of a TU without PCH
    output TEXT PRIMARY KEY, dur_ms INTEGER, run INTEGER, deps BLOB);
CREATE TABLE IF NOT EXISTS traces (         -- header parse events of a TU compile without PCH
    run INTEGER, output TEXT, target TEXT, compile_us INTEGER, headers BLOB,
    PRIMARY KEY (run, output));
CREATE TABLE IF NOT EXISTS ya (
    id INTEGER PRIMARY KEY, main_commit TEXT, kind TEXT, wall_ms INTEGER, cpu_ms INTEGER,
    note TEXT, added_at TEXT);
"""


# =============================================================================
# Helpers
# =============================================================================

def log(msg):
    print(msg, file=sys.stderr, flush=True)


def fmt(ms):
    if ms is None:
        return "?"
    s = ms / 1000.0
    sign = "-" if s < 0 else ""
    s = abs(s)
    if s >= 3600:
        return "%s%.1fh" % (sign, s / 3600)
    if s >= 60:
        return "%s%.1fm" % (sign, s / 60)
    return "%s%.1fs" % (sign, s)


def median(values):
    values = sorted(values)
    if not values:
        return None
    n = len(values)
    return values[n // 2] if n % 2 else (values[n // 2 - 1] + values[n // 2]) / 2.0


def pack(ids):
    return zlib.compress(array.array("i", ids).tobytes())


def unpack(blob, code="i"):
    a = array.array(code)
    a.frombytes(zlib.decompress(blob))
    return a


def git(root, *args):
    try:
        return subprocess.run(["git", "-C", root] + list(args), check=True, capture_output=True,
                              text=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


class Db(object):
    def __init__(self, path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.path = path
        self.c = sqlite3.connect(path)
        self.c.executescript(SCHEMA)
        self._ids = {}

    def name_id(self, name):
        i = self._ids.get(name)
        if i is None:
            row = self.c.execute("SELECT id FROM names WHERE name = ?", (name,)).fetchone()
            if row is None:
                i = self.c.execute("INSERT INTO names(name) VALUES (?)", (name,)).lastrowid
            else:
                i = row[0]
            self._ids[name] = i
        return i

    def names(self, ids):
        out = {}
        ids = list(set(ids))
        for k in range(0, len(ids), 500):
            chunk = ids[k:k + 500]
            q = "SELECT id, name FROM names WHERE id IN (%s)" % ",".join("?" * len(chunk))
            out.update(self.c.execute(q, chunk).fetchall())
        return out


def default_db(source_root):
    return os.path.join(source_root, DB_DIR, "stats.sqlite")


def roots_of(build_dir):
    source_root, sysroot = btr.detect_roots(build_dir)
    if source_root is None:
        sys.exit("%s: no build.ninja of gn" % build_dir)
    return os.path.realpath(source_root), btr.Paths(build_dir, source_root, sysroot)


def raw_path(paths, name):
    """A normalized name (btr.Paths.normalize) -> a file to stat."""
    if name.startswith("<out>/"):
        return os.path.join(paths.build_dir, name[6:])
    if name.startswith("<sysroot>/"):
        return os.path.join(paths.sysroot or "/", name[10:])
    if os.path.isabs(name):
        return name
    return os.path.join(paths.source_root, name)


# =============================================================================
# The build: log, manifest, deps
# =============================================================================

LogEntry = collections.namedtuple("LogEntry", "start end mtime output cmdhash")


def read_log(build_dir):
    entries = []
    with open(os.path.join(build_dir, ".ninja_log")) as f:
        if not f.readline().startswith("# ninja log"):
            sys.exit("unknown .ninja_log format")
        for line in f:
            p = line.rstrip("\n").split("\t")
            if len(p) >= 5:
                entries.append(LogEntry(int(p[0]), int(p[1]), int(p[2]), os.path.normpath(p[3]), p[4]))
    return entries


def run_boundaries(entries):
    """Indices where a new ninja run starts in a sequence of log entries (in log order):
    the end times only grow within a run, up to some jitter."""
    out = []
    for i in range(1, len(entries)):
        prev = entries[i - 1].end
        if entries[i].end < prev - max(1000, 0.2 * prev):
            out.append(i)
    return out


def last_run(db, build_dir, entries):
    """The entries of the last ninja run: the latest entry of each output that the previous
    `record` didn't see, those after the last run boundary among them."""
    state = dict(((o, (s, e, m, h)) for o, s, e, m, h in db.c.execute(
        "SELECT output, start_ms, end_ms, mtime, cmdhash FROM log_state WHERE build_dir = ?", (build_dir,))))
    latest = {}
    for i, e in enumerate(entries):
        latest[e.output] = i
    fresh = sorted(i for o, i in latest.items()
                   if state.get(o) != (entries[i].start, entries[i].end, entries[i].mtime, entries[i].cmdhash))
    seq = [entries[i] for i in fresh]
    cut = run_boundaries(seq)
    skipped = 0
    if cut:
        skipped = cut[-1]
        seq = seq[cut[-1]:]
    return seq, skipped, bool(state)


class Graph(object):
    def __init__(self, build_dir):
        self.edges, self.rules = btr.parse_ninja_manifest(build_dir)
        self.producer = {}
        for i, e in enumerate(self.edges):
            for o in e.outputs:
                self.producer[o] = i
        self._kinds = {}

    def kind(self, i):
        k = self._kinds.get(i)
        if k is None:
            e = self.edges[i]
            if any(o.endswith(".gch") for o in e.outputs):
                k = "pch"
            elif e.rule in COMPILE_RULES:
                k = "compile"
            elif e.rule in LINK_RULES:
                k = "link"
            elif "build.ninja" in e.outputs:
                k = "gn"
            elif e.rule in ("stamp", "copy", "phony"):
                k = e.rule
            else:
                k = btr.rule_kind(e.rule, self.rules)
            self._kinds[i] = k
        return k

    def uses_pch(self, i):
        e = self.edges[i]
        return e.rule in COMPILE_RULES and any(p.endswith(".gch") for p in e.inputs)


# =============================================================================
# record
# =============================================================================

def input_commands(graph, output, by_output):
    """This run's commands among the inputs of the command of `output`, through phony edges."""
    i = graph.producer.get(output)
    if i is None:
        return []
    out = []
    stack = list(graph.edges[i].inputs) + list(graph.edges[i].order_only)
    visited = set()
    while stack:
        p = stack.pop()
        if p in visited:
            continue
        visited.add(p)
        d = by_output.get(p)
        if d is not None:
            out.append(d)
            continue
        j = graph.producer.get(p)
        if j is not None and graph.edges[j].rule == "phony":
            stack.extend(graph.edges[j].inputs + graph.edges[j].order_only)
    return out


def command_index(cmds, graph):
    """Every output of this run's commands (a .so and its .TOC) -> its command."""
    by_output = {}
    for c in cmds:
        i = graph.producer.get(c["output"])
        for o in (graph.edges[i].outputs if i is not None else [c["output"]]):
            by_output[o] = c
    return by_output


def critical_chain(cmds, graph):
    """The chain of commands that ended the build: from the last one back through the input
    that finished last."""
    if not cmds:
        return []
    by_output = command_index(cmds, graph)
    cur = max(cmds, key=lambda c: c["end"])
    chain = []
    seen = set()
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        chain.append(cur)
        best = None
        for d in input_commands(graph, cur["output"], by_output):
            if d is not cur and d["end"] <= cur["start"] + 50 and (best is None or d["end"] > best["end"]):
                best = d
        cur = best
    chain.reverse()
    return chain


def ideal_path(cmds, graph):
    """The longest chain of this run's commands with unlimited parallelism."""
    by_output = command_index(cmds, graph)
    index = dict((id(c), k) for k, c in enumerate(cmds))
    deps = [set(index[id(d)] for d in input_commands(graph, c["output"], by_output) if d is not c) for c in cmds]
    finish = [None] * len(cmds)
    for root in range(len(cmds)):
        stack = [(root, False)]
        while stack:
            node, done = stack.pop()
            if finish[node] is not None:
                continue
            if not done:
                stack.append((node, True))
                stack.extend((d, False) for d in deps[node] if finish[d] is None)
                continue
            finish[node] = max([finish[d] or 0 for d in deps[node]] + [0]) + cmds[node]["cpu"]
    return max(finish) if finish else 0


def parallelism(cmds):
    """(max commands at once, time with at most a quarter of that, its commands)."""
    events = []
    for c in cmds:
        events.append((c["start"], 1))
        events.append((c["end"], -1))
    events.sort()
    cur = peak = 0
    for _, d in events:
        cur += d
        peak = max(peak, cur)
    low = 0
    low_spans = []
    cur = 0
    prev = None
    limit = max(1, peak // 4)
    for t, d in events:
        if prev is not None and t > prev and 0 < cur <= limit:
            low += t - prev
            low_spans.append((prev, t))
        cur += d
        prev = t
    return peak, low, low_spans


def stat_mtime(paths, name, cache):
    m = cache.get(name)
    if m is None:
        try:
            m = os.stat(raw_path(paths, name)).st_mtime_ns
        except OSError:
            m = -1
        cache[name] = m
    return m


def why(cmds, graph, deps, paths, state, prev_hash):
    """Why each command ran (ninja's own logic): the inputs newer than its previous output,
    a changed command line, no previous output. Sets c["direct"] (input names) and c["reason"]."""
    cache = {}
    for c in cmds:
        out = c["output"]
        prev = state.get(out)
        i = graph.producer.get(out)
        if prev is None:
            c["reason"], c["direct"] = "new", []
            continue
        if prev_hash.get(out) != c["cmdhash"]:
            c["reason"], c["direct"] = "command", []
            continue
        names = set()
        if i is not None:
            for p in graph.edges[i].inputs:
                names.add(paths.normalize(p))
        for p in deps.get(out, ()):
            names.add(p)
        newer = [n for n in names if stat_mtime(paths, n, cache) > prev]
        c["reason"] = "inputs" if newer else "dirty"
        c["direct"] = sorted(newer)


def root_causes(cmds, graph, paths):
    """Direct causes resolved to source files: a generated input changed because of what its
    command ran for."""
    by_output = dict((paths.normalize(c["output"]), c) for c in cmds)
    memo = {}

    def resolve(c, depth=0):
        key = c["output"]
        if key in memo:
            return memo[key]
        memo[key] = set()
        out = set()
        if c["reason"] in ("new", "command", "dirty"):
            out.add("<%s>" % c["reason"])
        for n in c["direct"]:
            d = by_output.get(n)
            if d is not None and depth < 50:
                out |= resolve(d, depth + 1)
            else:
                out.add(n)
        memo[key] = out
        return out

    for c in cmds:
        c["roots"] = resolve(c)


def load_traces(build_dir, paths, cmds, jobs):
    todo = []
    for c in cmds:
        if c["kind"] == "compile" and not c["pch"] and c["rule"] == "cxx":
            t = btr.trace_path_for(build_dir, c["output"])
            if t is not None:
                todo.append((c["output"], t))
    if not todo:
        return {}
    log("reading %d time traces..." % len(todo))
    out = {}
    if len(todo) > 50 and jobs > 1:
        with multiprocessing.Pool(jobs, initializer=btr._init_worker, initargs=(paths,)) as pool:
            results = pool.imap_unordered(btr.load_trace, todo, chunksize=16)
            for obj, data, err in results:
                if data is not None:
                    out[obj] = data
    else:
        btr._init_worker(paths)
        for job in todo:
            obj, data, err = btr.load_trace(job)
            if data is not None:
                out[obj] = data
    return out


def running_ninja(build_dir):
    """Pids of ninja processes building in build_dir."""
    out = []
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            with open("/proc/%s/cmdline" % pid, "rb") as f:
                argv = f.read().split(b"\0")
            if os.path.basename(argv[0].decode()) != "ninja":
                continue
            d = os.readlink("/proc/%s/cwd" % pid)
            for k, a in enumerate(argv):
                if a == b"-C" and k + 1 < len(argv):
                    d = os.path.join(d, argv[k + 1].decode())
                elif a.startswith(b"-C") and len(a) > 2:
                    d = os.path.join(d, a[2:].decode())
            if os.path.realpath(d) == build_dir:
                out.append(int(pid))
        except (OSError, UnicodeDecodeError):
            continue
    return out


def record(args):
    build_dir = os.path.realpath(args.build_dir)
    source_root, paths = roots_of(build_dir)
    busy = running_ninja(build_dir)
    if busy and not args.force:
        sys.exit("ninja is running in %s (pid %s): record after it ends (or --force)"
                 % (build_dir, ", ".join(map(str, busy))))
    db = Db(args.db or default_db(source_root))
    entries = read_log(build_dir)
    seq, skipped, known = last_run(db, build_dir, entries)
    if not seq:
        log("no commands since the last record")
        return
    if skipped:
        log("note: %d earlier commands not recorded (an unrecorded run before the last one)" % skipped)
    if not known:
        log("note: the first record of %s: why commands ran is unknown" % build_dir)

    log("reading the ninja manifest...")
    graph = Graph(build_dir)
    log("reading .ninja_deps...")
    deps_raw, deps_paths = btr.parse_ninja_deps(build_dir)

    def deps_of(out):
        ids = deps_raw.get(out)
        return [paths.normalize(deps_paths[k]) for k in ids] if ids is not None else []

    state = {}
    prev_hash = {}
    for o, m, h in db.c.execute("SELECT output, mtime, cmdhash FROM log_state WHERE build_dir = ?", (build_dir,)):
        state[o] = m
        prev_hash[o] = h

    cmds = []
    for e in seq:
        i = graph.producer.get(e.output)
        kind = graph.kind(i) if i is not None else "?"
        cmds.append({
            "output": e.output, "start": e.start, "end": e.end, "cpu": max(0, e.end - e.start),
            "cmdhash": e.cmdhash, "mtime": e.mtime, "kind": kind,
            "rule": graph.edges[i].rule if i is not None else "?",
            "target": graph.edges[i].target if i is not None else "?",
            "pch": 1 if i is not None and graph.uses_pch(i) else 0,
        })
    # one command, several outputs (.so and .so.TOC): keep the first output only
    seen_edges = set()
    uniq = []
    for c in cmds:
        i = graph.producer.get(c["output"])
        key = (i, c["start"], c["end"]) if i is not None else c["output"]
        if key in seen_edges:
            continue
        seen_edges.add(key)
        uniq.append(c)
    cmds = uniq

    deps = dict((c["output"], deps_of(c["output"])) for c in cmds)
    why(cmds, graph, deps, paths, state, prev_hash)
    root_causes(cmds, graph, paths)

    wall = max(c["end"] for c in cmds)
    cpu = sum(c["cpu"] for c in cmds)
    starts = [c["mtime"] / 1e6 - c["end"] for c in cmds if c["mtime"] > 0]
    started_at = (median(starts) or 0) / 1000.0
    chain = critical_chain(cmds, graph)
    ideal = ideal_path(cmds, graph)
    peak, low, low_spans = parallelism(cmds)

    # machine load: TUs compiled without PCH now and before
    prev_dur = dict(db.c.execute("SELECT output, dur_ms FROM nopch"))
    ratios = [c["cpu"] / float(prev_dur[c["output"]]) for c in cmds
              if c["kind"] == "compile" and not c["pch"] and prev_dur.get(c["output"], 0) >= 2000
              and c["reason"] != "command"]
    load_factor = median(ratios) if len(ratios) >= 10 else None

    # PCH balance
    pch = pch_balance(db, cmds, prev_dur, load_factor or 1.0)

    # remaining work
    remaining = remaining_cpu = None
    if args.targets is not None:
        remaining, remaining_cpu = left_to_build(build_dir, args.targets, db)

    head = git(source_root, "rev-parse", "HEAD")
    dirty = git(source_root, "status", "--porcelain", "--untracked-files=no")
    main = git(source_root, "merge-base", "HEAD", args.main_ref)
    main_date = main_subject = None
    if main:
        info = git(source_root, "show", "-s", "--format=%cI%x09%s", main) or ""
        main_date, _, main_subject = info.partition("\t")

    cur = db.c.execute(
        "INSERT INTO runs(recorded_at, build_dir, started_at, wall_ms, cpu_ms, commands, status, remaining,"
        " remaining_cpu_ms, git_head, git_dirty, main_commit, main_date, main_subject, label, load_factor,"
        " critical_ms, ideal_ms, max_parallel, low_parallel_ms, pch_build_ms, pch_tus, pch_saved_ms,"
        " pch_extra_ms, pch_estimated, pch_unknown) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (datetime.datetime.now().isoformat(timespec="seconds"), build_dir, started_at, wall, cpu, len(cmds),
         args.status, remaining, remaining_cpu, head, 1 if dirty else 0, main, main_date, main_subject,
         args.label, load_factor, (chain[-1]["end"] - chain[0]["start"]) if chain else 0, ideal, peak, low,
         pch["build_ms"], pch["tus"], pch["saved_ms"], pch["extra_ms"], pch["estimated"], pch["unknown"]))
    run = cur.lastrowid

    db.c.executemany("INSERT INTO commands VALUES (?,?,?,?,?,?,?,?,?)",
                     [(run, c["output"], c["target"], c["kind"], c["start"], c["end"], c["cmdhash"], c["pch"],
                       c["reason"]) for c in cmds])
    db.c.executemany("INSERT INTO pch_targets VALUES (?,?,?,?,?,?)",
                     [(run, t, v["tus"], v["build_ms"], v["extra_ms"], v["saved_ms"])
                      for t, v in pch["per_target"].items()])
    db.c.executemany("INSERT INTO critical VALUES (?,?,?,?,?)",
                     [(run, k, c["output"], c["start"], c["end"]) for k, c in enumerate(chain)])
    agg = collections.defaultdict(lambda: [0, 0])
    for c in cmds:
        for n in c["direct"]:
            a = agg[(n, 1)]
            a[0] += 1
            a[1] += c["cpu"]
        for n in c["roots"]:
            a = agg[(n, 0)]
            a[0] += 1
            a[1] += c["cpu"]
    db.c.executemany("INSERT INTO causes VALUES (?,?,?,?,?)",
                     [(run, db.name_id(n), direct, k, ms) for (n, direct), (k, ms) in agg.items()])

    # TUs without PCH: duration and dependencies, the reference for the PCH balance
    rows = []
    for c in cmds:
        if c["kind"] == "compile" and not c["pch"]:
            rows.append((c["output"], c["cpu"], run, pack([db.name_id(n) for n in deps[c["output"]]])))
    db.c.executemany("INSERT OR REPLACE INTO nopch VALUES (?,?,?,?)", rows)

    if not args.no_traces:
        traces = load_traces(build_dir, paths, cmds, args.jobs)
        trows = []
        for c in cmds:
            t = traces.get(c["output"])
            if t is None:
                continue
            names, ids, ts, dur = t["headers"]
            a_ids, a_ts, a_dur = array.array("i"), array.array("q"), array.array("q")
            a_ids.frombytes(ids)
            a_ts.frombytes(ts)
            a_dur.frombytes(dur)
            keep = [k for k in range(len(a_ids)) if a_dur[k] >= MIN_HEADER_US]
            blob = zlib.compress(array.array("q", [v for k in keep for v in
                                                   (db.name_id(names[a_ids[k]]), a_ts[k], a_dur[k])]).tobytes())
            trows.append((run, c["output"], c["target"], t["totals"].get("ExecuteCompiler", 0), blob))
        db.c.executemany("INSERT OR REPLACE INTO traces VALUES (?,?,?,?,?)", trows)

    # what the next record compares with
    latest = {}
    for e in entries:
        latest[e.output] = e
    db.c.execute("DELETE FROM log_state WHERE build_dir = ?", (build_dir,))
    db.c.executemany("INSERT INTO log_state VALUES (?,?,?,?,?,?)",
                     [(build_dir, e.output, e.start, e.end, e.mtime, e.cmdhash) for e in latest.values()])
    db.c.commit()

    text = report(db, run, low_spans=low_spans, top=args.top)
    path = os.path.join(os.path.dirname(db.path), "reports", "run-%d.md" % run)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(text)
    print(summary(db, run))
    print("full report: %s" % path)


def pch_balance(db, cmds, prev_dur, load_factor):
    """What PCH won (+) or lost (-) in this run against the same build without it:
    - a TU with PCH saves its duration without PCH (the last known, scaled by the machine
      load) minus its duration now; one rebuilt only because a header of the PCH changed
      (none of the changed files is among its dependencies without PCH) loses all of it;
    - building a PCH loses its time.
    TUs never compiled without PCH get the median ratio of their target (or of the run)."""
    pch_cmds = [c for c in cmds if c["kind"] == "compile" and c["pch"]]
    out = {"build_ms": sum(c["cpu"] for c in cmds if c["kind"] == "pch"), "tus": len(pch_cmds),
           "saved_ms": 0, "extra_ms": 0, "estimated": 0, "unknown": 0, "per_target": {}}
    if not pch_cmds and not out["build_ms"]:
        return out
    nopch_deps = {}
    for c in pch_cmds:
        row = db.c.execute("SELECT deps FROM nopch WHERE output = ?", (c["output"],)).fetchone()
        if row is not None:
            nopch_deps[c["output"]] = set(unpack(row[0]))
    ratios = collections.defaultdict(list)
    for c in pch_cmds:
        b = prev_dur.get(c["output"])
        if b:
            ratios[c["target"]].append(c["cpu"] / (b * load_factor))
    all_ratios = [r for v in ratios.values() for r in v]
    per_target = collections.defaultdict(lambda: {"tus": 0, "saved_ms": 0, "extra_ms": 0, "build_ms": 0})
    for c in cmds:
        if c["kind"] == "pch":
            per_target[c["target"]]["build_ms"] += c["cpu"]
            out["saved_ms"] -= c["cpu"]
            per_target[c["target"]]["saved_ms"] -= c["cpu"]
    for c in pch_cmds:
        t = per_target[c["target"]]
        t["tus"] += 1
        own = nopch_deps.get(c["output"])
        sources = [n for n in c["direct"] if not n.startswith("<out>/")] if c["reason"] == "inputs" else None
        if own is not None and sources:
            ids = set(db.name_id(n) for n in sources)
            if not ids & own:
                out["extra_ms"] += c["cpu"]
                t["extra_ms"] += c["cpu"]
                out["saved_ms"] -= c["cpu"]
                t["saved_ms"] -= c["cpu"]
                continue
        b = prev_dur.get(c["output"])
        if b:
            without = b * load_factor
        else:
            r = median(ratios.get(c["target"]) or all_ratios)
            if not r:
                out["unknown"] += 1
                continue
            out["estimated"] += 1
            without = c["cpu"] / r
        out["saved_ms"] += int(without - c["cpu"])
        t["saved_ms"] += int(without - c["cpu"])
    out["per_target"] = dict(per_target)
    return out


def left_to_build(build_dir, targets, db):
    """Commands ninja would still run for `targets` and their CPU time by the last known durations."""
    try:
        res = subprocess.run(["ninja", "-C", build_dir, "-n"] + targets, capture_output=True, text=True)
    except OSError:
        return None, None
    lines = [l for l in res.stdout.splitlines() if re.match(r"\[\d+/\d+\] ", l)]
    cpu = 0
    for l in lines:
        out = l.split()[-1]
        out = os.path.normpath(out)
        row = db.c.execute("SELECT end_ms - start_ms FROM commands WHERE output = ? ORDER BY run DESC LIMIT 1",
                           (out,)).fetchone()
        if row:
            cpu += row[0]
    return len(lines), cpu


# =============================================================================
# Reports
# =============================================================================

def run_row(db, run):
    db.c.row_factory = sqlite3.Row
    row = db.c.execute("SELECT * FROM runs WHERE id = ?", (run,)).fetchone()
    db.c.row_factory = None
    if row is None:
        sys.exit("no run %s" % run)
    return row


def status_text(r):
    if r["status"] is None and r["remaining"] is None:
        return "status unknown"
    if (r["status"] or 0) == 0 and not r["remaining"]:
        return "ok"
    s = "FAILED" if r["status"] else "incomplete"
    if r["remaining"]:
        s += ", %d commands left (~%s CPU)" % (r["remaining"], fmt(r["remaining_cpu_ms"]))
    return s


def summary(db, run):
    r = run_row(db, run)
    kinds = db.c.execute("SELECT kind, COUNT(*), SUM(end_ms - start_ms) FROM commands WHERE run = ?"
                         " GROUP BY kind ORDER BY 3 DESC", (run,)).fetchall()
    out = ["run %d: %s, %d commands, wall %s, CPU %s (x%.1f), %s" % (
        run, (r["main_commit"] or "?")[:10], r["commands"], fmt(r["wall_ms"]), fmt(r["cpu_ms"]),
        r["cpu_ms"] / max(1.0, r["wall_ms"]), status_text(r))]
    out.append("  " + ", ".join("%s %d (%s)" % (k, n, fmt(ms)) for k, n, ms in kinds[:6]))
    out.append("  critical chain %s, ideal %s, peak %d jobs, low parallelism %s" % (
        fmt(r["critical_ms"]), fmt(r["ideal_ms"]), r["max_parallel"], fmt(r["low_parallel_ms"])))
    if r["pch_tus"] or r["pch_build_ms"]:
        out.append("  PCH: %+.1fs (%d TUs, PCH builds %s, extra rebuilds %s%s)" % (
            r["pch_saved_ms"] / 1000.0, r["pch_tus"], fmt(r["pch_build_ms"]), fmt(r["pch_extra_ms"]),
            ", %d estimated" % r["pch_estimated"] if r["pch_estimated"] else "")
                   + ("; %d TUs without a reference, not counted (see `baseline`)" % r["pch_unknown"]
                      if r["pch_unknown"] else ""))
    causes = db.c.execute("SELECT n.name, c.commands, c.cpu_ms FROM causes c JOIN names n ON n.id = c.name"
                          " WHERE c.run = ? AND c.direct = 0 ORDER BY c.cpu_ms DESC LIMIT 5", (run,)).fetchall()
    if causes:
        out.append("  why: " + "; ".join("%s %d (%s)" % (n, k, fmt(ms)) for n, k, ms in causes))
    return "\n".join(out)


def report(db, run, low_spans=None, top=30):
    r = run_row(db, run)
    out = []
    w = out.append
    w("# Build %d" % run)
    w("")
    w("| | |")
    w("|---|---|")
    w("| recorded | %s |" % r["recorded_at"])
    w("| build dir | `%s` |" % r["build_dir"])
    w("| HEAD | `%s`%s |" % (r["git_head"], " (dirty)" if r["git_dirty"] else ""))
    w("| main | `%s` %s %s |" % (r["main_commit"], r["main_date"] or "", (r["main_subject"] or "").replace("|", "/")))
    w("| status | %s |" % status_text(r))
    w("| commands | %d |" % r["commands"])
    w("| wall | %s |" % fmt(r["wall_ms"]))
    w("| CPU | %s (x%.1f of wall) |" % (fmt(r["cpu_ms"]), r["cpu_ms"] / max(1.0, r["wall_ms"])))
    w("| machine load | %s |" % ("x%.2f of the previous TU times" % r["load_factor"] if r["load_factor"] else "unknown"))
    w("")

    w("## Commands by kind")
    w("")
    rows = db.c.execute("SELECT kind, COUNT(*), SUM(end_ms - start_ms), MAX(end_ms - start_ms) FROM commands"
                        " WHERE run = ? GROUP BY kind ORDER BY 3 DESC", (run,)).fetchall()
    btr.md_table(out, ["kind", "commands", "CPU", "longest"], [(k, n, fmt(s), fmt(m)) for k, n, s, m in rows])
    w("")

    w("## Critical path")
    w("")
    w("The chain that ended the build (each command waited for the previous one): %s; the longest chain "
      "with unlimited parallelism: %s. Peak %d commands at once; %s with at most a quarter of that."
      % (fmt(r["critical_ms"]), fmt(r["ideal_ms"]), r["max_parallel"], fmt(r["low_parallel_ms"])))
    w("")
    chain = db.c.execute("SELECT c.output, c.start_ms, c.end_ms, k.kind, k.target FROM critical c"
                         " LEFT JOIN commands k ON k.run = c.run AND k.output = c.output"
                         " WHERE c.run = ? ORDER BY c.pos", (run,)).fetchall()
    rows = []
    prev_end = None
    for o, s, e, kind, target in chain:
        wait = s - prev_end if prev_end is not None else s
        rows.append((o, kind, target, fmt(e - s), fmt(wait), fmt(e)))
        prev_end = e
    btr.md_table(out, ["output", "kind", "target", "duration", "waited before", "finished at"], rows)
    w("")
    if low_spans:
        w("Longest commands running while the parallelism was low:")
        w("")
        cmds = db.c.execute("SELECT output, kind, start_ms, end_ms FROM commands WHERE run = ?", (run,)).fetchall()
        low_cmd = collections.Counter()
        for o, kind, s, e in cmds:
            for a, b in low_spans:
                if s < b and e > a:
                    low_cmd[(o, kind, e - s)] += min(e, b) - max(s, a)
        btr.md_table(out, ["output", "kind", "duration", "in low parallelism"],
                     [(o, k, fmt(d), fmt(t)) for (o, k, d), t in low_cmd.most_common(10)])
        w("")

    w("## Why the commands ran")
    w("")
    w("A command runs when an input is newer than its previous output, its command line changed, or there "
      "was no output. Root causes: generated inputs are resolved to what made their own commands run. "
      "A command counts for each of its causes.")
    w("")
    reasons = db.c.execute("SELECT reason, COUNT(*), SUM(end_ms - start_ms) FROM commands WHERE run = ?"
                           " GROUP BY reason ORDER BY 3 DESC", (run,)).fetchall()
    btr.md_table(out, ["reason", "commands", "CPU"], [(k or "?", n, fmt(s)) for k, n, s in reasons])
    w("")
    causes = db.c.execute("SELECT n.name, c.commands, c.cpu_ms FROM causes c JOIN names n ON n.id = c.name"
                          " WHERE c.run = ? AND c.direct = 0 ORDER BY c.cpu_ms DESC LIMIT ?", (run, top)).fetchall()
    btr.md_table(out, ["changed", "commands", "CPU"], [(n, k, fmt(ms)) for n, k, ms in causes])
    w("")

    if r["pch_tus"] or r["pch_build_ms"]:
        w("## PCH")
        w("")
        w("Against the same build without PCH: a TU saves its duration without PCH (the last known one, "
          "scaled by the machine load) minus its duration now; a TU rebuilt only because a header of the PCH "
          "changed loses all its time; building a PCH loses its time. %d of %d TUs had no duration without "
          "PCH and got the median ratio of their target." % (r["pch_estimated"], r["pch_tus"]))
        w("")
        w("Total: **%+.1fs** (PCH builds %s, extra rebuilds %s)." % (
            r["pch_saved_ms"] / 1000.0, fmt(r["pch_build_ms"]), fmt(r["pch_extra_ms"])))
        if r["pch_unknown"]:
            w("")
            w("**%d TUs with PCH have no reference** (never compiled without PCH, no ratio of their target): "
              "their gain is not counted. Record a build without PCH (`pch_mode = "none"`, another build dir) "
              "and load it with `baseline`." % r["pch_unknown"])
        w("")
        per = db.c.execute("SELECT target, tus, build_ms, extra_ms, saved_ms FROM pch_targets WHERE run = ?"
                           " ORDER BY saved_ms", (run,)).fetchall()
        if per:
            btr.md_table(out, ["target", "TUs", "PCH build", "extra rebuilds", "balance"],
                         [(t, n, fmt(b), fmt(x), "%+.1fs" % (v / 1000.0)) for t, n, b, x, v in per])
            w("")

    w("## Slower than the last time")
    w("")
    w("Commands at least x%.1f and %s slower than their previous run (any recorded run), "
      "the machine load taken into account." % (REGRESSION_RATIO, fmt(REGRESSION_MIN_MS)))
    w("")
    lf = r["load_factor"] or 1.0
    rows = []
    for o, kind, s, e, pch in db.c.execute("SELECT output, kind, start_ms, end_ms, pch FROM commands WHERE run = ?",
                                           (run,)).fetchall():
        prev = db.c.execute("SELECT end_ms - start_ms, pch FROM commands WHERE output = ? AND run < ?"
                            " ORDER BY run DESC LIMIT 1", (o, run)).fetchone()
        if prev is None or prev[1] != pch:
            continue
        d, p = e - s, prev[0] * lf
        if d >= REGRESSION_RATIO * p and d - p >= REGRESSION_MIN_MS:
            rows.append((d - p, o, kind, fmt(prev[0]), fmt(d)))
    rows.sort(reverse=True)
    btr.md_table(out, ["output", "kind", "before", "now"], [x[1:] for x in rows[:top]])
    w("")

    w("## Longest commands")
    w("")
    rows = db.c.execute("SELECT output, kind, target, end_ms - start_ms FROM commands WHERE run = ?"
                        " ORDER BY 4 DESC LIMIT ?", (run, top)).fetchall()
    btr.md_table(out, ["output", "kind", "target", "duration"], [(o, k, t, fmt(d)) for o, k, t, d in rows])
    w("")
    rows = db.c.execute("SELECT target, COUNT(*), SUM(end_ms - start_ms) FROM commands WHERE run = ?"
                        " GROUP BY target ORDER BY 3 DESC LIMIT ?", (run, top)).fetchall()
    w("By gn target:")
    w("")
    btr.md_table(out, ["target", "commands", "CPU"], [(t, n, fmt(s)) for t, n, s in rows])
    w("")
    return "\n".join(out)


def show(args):
    db = Db(args.db or default_db(os.getcwd()))
    run = args.run or db.c.execute("SELECT MAX(id) FROM runs").fetchone()[0]
    if run is None:
        sys.exit("no runs")
    print(summary(db, run))
    print()
    print(report(db, run, top=args.top))


# =============================================================================
# pch: candidates over recorded runs
# =============================================================================

class TraceTU(object):
    __slots__ = ("h_ids", "h_ts", "h_dur")


def pch_candidates(args):
    build_dir = os.path.realpath(args.build_dir)
    source_root, paths = roots_of(build_dir)
    db = Db(args.db or default_db(source_root))
    runs = [r for r, in db.c.execute("SELECT id FROM runs ORDER BY id DESC" + (" LIMIT %d" % args.last if args.last else ""))]
    if not runs:
        sys.exit("no runs")
    window = set(runs)
    log("reading the ninja manifest...")
    graph = Graph(build_dir)
    tus_of = collections.defaultdict(list)
    has_pch = set()
    for i, e in enumerate(graph.edges):
        if e.rule == "cxx":
            tus_of[e.target].append(e.outputs[0])
            if graph.uses_pch(i):
                has_pch.add(e.target)

    # rebuilds of every TU in the window, and the direct causes of each run
    rebuilt = collections.defaultdict(list)       # output -> [(run, ms)]
    for run, o, s, e in db.c.execute("SELECT run, output, start_ms, end_ms FROM commands WHERE kind = 'compile'"
                                     " AND run IN (%s)" % ",".join(map(str, runs))):
        rebuilt[o].append((run, e - s))
    changed = collections.defaultdict(set)        # run -> name ids changed (direct causes)
    for run, n in db.c.execute("SELECT run, name FROM causes WHERE direct = 1 AND run IN (%s)" % ",".join(map(str, runs))):
        changed[run].add(n)

    def latest_trace(o):
        row = db.c.execute("SELECT headers FROM traces WHERE output = ? ORDER BY run DESC LIMIT 1", (o,)).fetchone()
        if row is None:
            return None
        a = unpack(row[0], "q")
        tu = TraceTU()
        tu.h_ids = a[0::3]
        tu.h_ts = a[1::3]
        tu.h_dur = a[2::3]
        return tu

    write = set(args.write or ())
    unknown = write - set(tus_of)
    if unknown:
        sys.exit("no C++ TUs of %s in the manifest" % ", ".join(sorted(unknown)))
    rows = []
    for target, outs in tus_of.items():
        if target not in write and (target in has_pch or len(outs) < args.min_tus):
            continue
        if target not in write and not any(o in rebuilt for o in outs):
            continue
        traces = dict((o, latest_trace(o)) for o in outs)
        traces = dict((o, t) for o, t in traces.items() if t is not None)
        if len(traces) < args.min_tus:
            if target in write:
                sys.exit("%s: time traces of %d of its %d TUs (compiled without PCH, recorded)"
                         % (target, len(traces), len(outs)))
            continue
        presence = collections.Counter()
        for t in traces.values():
            presence.update(set(t.h_ids))
        selected = set(h for h, c in presence.items() if c >= args.coverage * len(traces))
        if not args.include_own:
            own = target[2:].split(":")[0] + "/"
            names = db.names(selected)
            selected = set(h for h in selected
                           if not names[h].startswith((own, "<out>/gen/" + own)))
        if not selected:
            continue
        parse = {}
        for o, t in traces.items():
            parse[o] = sum(t.h_dur[i] for i in btr.outermost_events(t, selected)) / 1000.0
        gch = max(parse.values())
        saved = lost = 0.0
        runs_with = 0
        for run in runs:
            built = [o for o in outs if any(r == run for r, _ in rebuilt.get(o, ()))]
            if not built:
                continue
            runs_with += 1
            saved += sum(parse.get(o, 0) * (1 - args.load_cost) for o in built)
            if changed[run] & selected or len(built) == len(outs):
                lost += gch
                # a header of the PCH changed: every TU of the target would be rebuilt
                if changed[run] & selected:
                    for o in outs:
                        if o not in built:
                            d = [ms for r, ms in rebuilt.get(o, ())]
                            lost += max(0.0, (d[-1] if d else 0) - parse.get(o, 0))
        net = saved - lost
        if target in write:
            path = write_pch_header(db, source_root, target, traces, selected)
            print("%s: %s (%+.1fs over the runs)" % (target, os.path.relpath(path, source_root), net / 1000.0))
        if net > 0:
            roots = collections.Counter()
            for t in traces.values():
                for i in btr.outermost_events(t, selected):
                    roots[t.h_ids[i]] += t.h_dur[i]
            rows.append((net, target, len(outs), runs_with, saved, lost, roots.most_common(args.roots)))
    rows.sort(reverse=True)
    print("PCH candidates over %d runs (%s), coverage %.0f%%, load cost %.2f:" % (
        len(runs), "all" if not args.last else "last %d" % args.last, 100 * args.coverage, args.load_cost))
    for net, target, n, rw, saved, lost, roots in rows[:args.top]:
        print("  %-60s %+8.1fs  (%d TUs, rebuilt in %d runs, saved %s, lost %s)" % (
            target, net / 1000.0, n, rw, fmt(saved), fmt(lost)))
        names = db.names([h for h, _ in roots])
        for h, us in roots:
            print("      %-70s %s" % (names[h], fmt(us / 1000.0)))

    # the PCH there are: their balance over the window
    rows = db.c.execute("SELECT r.id, r.pch_saved_ms FROM runs r WHERE r.id IN (%s) AND (r.pch_tus > 0 OR r.pch_build_ms > 0)"
                        % ",".join(map(str, runs))).fetchall()
    if rows:
        print("existing PCH over the same runs: %+.1fs in %d runs" % (sum(ms for _, ms in rows) / 1000.0, len(rows)))


PCH_DIR = "build/gn/pch"   # <dir>/<name>.h of target //<dir>:<name>, see library() in //build/gn/base.gni
LIBCXX_INCLUDE = "contrib/libs/cxxsupp/libcxx/include/"


def pch_include(name):
    """A header as the PCH includes it, or None (not includable on its own)."""
    if name.startswith("<sysroot>/") or os.path.isabs(name):
        return None
    if name.startswith("<out>/gen/"):
        name = name[len("<out>/gen/"):]
    elif name.startswith("<out>/"):
        return None
    if name.startswith(LIBCXX_INCLUDE):
        name = name[len(LIBCXX_INCLUDE):]
        if name.startswith("__") or "/__" in name:
            return None     # libc++ internals: a public header includes them
        return "<%s>" % name
    if not name.endswith((".h", ".hh", ".hpp", ".hxx")):
        return None
    return '"%s"' % name


def write_pch_header(db, source_root, target, traces, selected):
    """The PCH of `target`: the chosen headers not nested in other chosen ones,
    in the order the TUs include them."""
    order = collections.defaultdict(list)
    for t in traces.values():
        for rank, i in enumerate(btr.outermost_events(t, selected)):
            order[t.h_ids[i]].append(rank)
    names = db.names(order)
    lines, seen = [], set()
    for h in sorted(order, key=lambda h: (sum(order[h]) / len(order[h]), names[h])):
        inc = pch_include(names[h])
        if inc is not None and inc not in seen:
            seen.add(inc)
            lines.append("#include %s" % inc)
    d, _, name = target[2:].partition(":")
    path = os.path.join(source_root, PCH_DIR, d, name + ".h")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write("#pragma once\n\n" + "\n".join(lines) + "\n")
    return path


# =============================================================================
# cost: what touching files would rebuild
# =============================================================================

def cost(args):
    build_dir = os.path.realpath(args.build_dir)
    source_root, paths = roots_of(build_dir)
    db = Db(args.db or default_db(source_root))
    deps, dep_paths = btr.parse_ninja_deps(build_dir)
    wanted = set()
    for f in args.files:
        p = os.path.relpath(os.path.realpath(f), source_root)
        wanted.add(p)
    ids = set(k for k, p in enumerate(dep_paths) if paths.normalize(p) in wanted)
    hit = [o for o, v in deps.items() if ids & set(v)]
    graph = Graph(build_dir)
    for i, e in enumerate(graph.edges):
        if e.rule in COMPILE_RULES and any(paths.normalize(p) in wanted for p in e.inputs):
            hit.append(e.outputs[0])
    hit = sorted(set(hit))
    total = 0
    longest = 0
    unknown = 0
    targets = collections.Counter()
    logged = {}
    for e in read_log(build_dir):
        logged[e.output] = e.end - e.start
    for o in hit:
        row = db.c.execute("SELECT end_ms - start_ms FROM commands WHERE output = ? ORDER BY run DESC LIMIT 1",
                           (o,)).fetchone()
        d = row[0] if row is not None else logged.get(o)
        i = graph.producer.get(o)
        if i is not None:
            targets[graph.edges[i].target] += 1
        if d is None:
            unknown += 1
            continue
        total += d
        longest = max(longest, d)
    print("%d commands to rerun (%d without a known duration), CPU %s, the longest %s; with -j%d at least %s" % (
        len(hit), unknown, fmt(total), fmt(longest), args.jobs, fmt(max(longest, total / max(1, args.jobs)))))
    print("in %d gn targets (each relinks its library):" % len(targets))
    for t, n in targets.most_common(args.top):
        print("  %-70s %d" % (t, n))


# =============================================================================
# ya, series, baseline
# =============================================================================

def ya_add(args):
    source_root = git(os.getcwd(), "rev-parse", "--show-toplevel") or os.getcwd()
    db = Db(args.db or default_db(source_root))
    ref = args.main or git(source_root, "merge-base", "HEAD", args.main_ref)
    main = git(source_root, "rev-parse", ref) or ref
    db.c.execute("INSERT INTO ya(main_commit, kind, wall_ms, cpu_ms, note, added_at) VALUES (?,?,?,?,?,?)",
                 (main, args.kind, int(args.wall * 1000), int(args.cpu * 1000) if args.cpu is not None else None,
                  args.note, datetime.datetime.now().isoformat(timespec="seconds")))
    db.c.commit()
    print("ya %s build of %s: wall %s" % (args.kind, main[:10], fmt(args.wall * 1000)))


def series(args):
    source_root = git(os.getcwd(), "rev-parse", "--show-toplevel") or os.getcwd()
    db = Db(args.db or default_db(source_root))
    db.c.row_factory = sqlite3.Row
    runs = db.c.execute("SELECT * FROM runs WHERE main_commit IS NOT NULL" +
                        (" AND label = ?" if args.label else "") + " ORDER BY id",
                        (args.label,) if args.label else ()).fetchall()
    ya = collections.defaultdict(list)
    for row in db.c.execute("SELECT * FROM ya ORDER BY id"):
        ya[row["main_commit"]].append(row)
    db.c.row_factory = None
    out = []
    w = out.append
    w("# gn vs ya make by main commit")
    w("")
    rows = []
    tot = collections.Counter()
    for r in runs:
        y = ya.get(r["main_commit"])
        y = y[-1] if y else None
        gwall = r["wall_ms"]
        rows.append(("`%s`" % r["main_commit"][:10], r["main_date"] or "", (r["main_subject"] or "")[:60],
                     r["id"], status_text(r), r["commands"], fmt(gwall), fmt(r["cpu_ms"]),
                     "%+.1fs" % (r["pch_saved_ms"] / 1000.0) if r["pch_tus"] or r["pch_build_ms"] else "",
                     fmt(y["wall_ms"]) if y else "", fmt(y["cpu_ms"]) if y and y["cpu_ms"] is not None else "",
                     fmt(y["wall_ms"] - gwall) if y else ""))
        tot["gn_wall"] += gwall
        tot["gn_cpu"] += r["cpu_ms"]
        tot["pch"] += r["pch_saved_ms"] or 0
        if y:
            tot["pairs"] += 1
            tot["ya_wall"] += y["wall_ms"]
            tot["gn_wall_paired"] += gwall
    btr.md_table(out, ["main", "date", "subject", "run", "status", "commands", "gn wall", "gn CPU", "PCH",
                       "ya wall", "ya CPU", "ya - gn"], rows)
    w("")
    w("Runs: %d, gn wall %s, gn CPU %s, PCH %+.1fs." % (len(runs), fmt(tot["gn_wall"]), fmt(tot["gn_cpu"]),
                                                       tot["pch"] / 1000.0))
    if tot["pairs"]:
        w("With ya results: %d, ya wall %s, gn wall %s: **gn saved %s**." % (
            tot["pairs"], fmt(tot["ya_wall"]), fmt(tot["gn_wall_paired"]),
            fmt(tot["ya_wall"] - tot["gn_wall_paired"])))
    text = "\n".join(out)
    if args.output:
        with open(args.output, "w") as f:
            f.write(text + "\n")
        print(args.output)
    else:
        print(text)


def baseline(args):
    """Durations and dependencies of the TUs of a build without PCH (another build dir)."""
    build_dir = os.path.realpath(args.build_dir)
    source_root, paths = roots_of(build_dir)
    db = Db(args.db or default_db(git(os.getcwd(), "rev-parse", "--show-toplevel") or source_root))
    entries = read_log(build_dir)
    graph = Graph(build_dir)
    deps, dep_paths = btr.parse_ninja_deps(build_dir)
    latest = {}
    for e in entries:
        latest[e.output] = e
    rows = []
    for o, e in latest.items():
        i = graph.producer.get(o)
        if i is None or graph.edges[i].rule not in COMPILE_RULES or graph.uses_pch(i):
            continue
        ids = deps.get(o, ())
        rows.append((o, e.end - e.start, None, pack([db.name_id(paths.normalize(dep_paths[k])) for k in ids])))
    # only TUs nothing newer is known for
    known = set(o for o, in db.c.execute("SELECT output FROM nopch"))
    rows = [r for r in rows if args.replace or r[0] not in known]
    db.c.executemany("INSERT OR REPLACE INTO nopch VALUES (?,?,?,?)", rows)
    db.c.commit()
    print("%d TUs without PCH from %s" % (len(rows), build_dir))


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--db", help="default: <source root>/%s/stats.sqlite" % DB_DIR)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("record", help="record the last ninja run")
    p.add_argument("build_dir")
    p.add_argument("--status", type=int, help="ninja exit code")
    p.add_argument("--targets", nargs="*", help="check what is left to build for these targets")
    p.add_argument("--label", help="a name of a series (e.g. full-test)")
    p.add_argument("--main-ref", default="origin/main")
    p.add_argument("--no-traces", action="store_true", help="don't read time traces (no data for `pch`)")
    p.add_argument("--force", action="store_true", help="even while ninja is running there")
    p.add_argument("-j", "--jobs", type=int, default=min(8, multiprocessing.cpu_count()),
                   help="processes reading time traces")
    p.add_argument("--top", type=int, default=30)
    p.set_defaults(func=record)

    p = sub.add_parser("show", help="the report of a recorded run")
    p.add_argument("run", type=int, nargs="?")
    p.add_argument("--top", type=int, default=30)
    p.set_defaults(func=show)

    p = sub.add_parser("pch", help="PCH candidates over recorded runs")
    p.add_argument("build_dir")
    p.add_argument("--last", type=int, help="the last N runs (default: all)")
    p.add_argument("--min-tus", type=int, default=4)
    p.add_argument("--coverage", type=float, default=0.75, help="a header included by this share of the TUs")
    p.add_argument("--load-cost", type=float, default=0.15)
    p.add_argument("--include-own", action="store_true", help="the target's own headers too")
    p.add_argument("--roots", type=int, default=8, help="headers shown per target")
    p.add_argument("--write", nargs="+", metavar="TARGET",
                   help="write the PCH of these targets to %s/<dir>/<name>.h" % PCH_DIR)
    p.add_argument("--top", type=int, default=20)
    p.set_defaults(func=pch_candidates)

    p = sub.add_parser("cost", help="what touching files would rebuild")
    p.add_argument("build_dir")
    p.add_argument("files", nargs="+")
    p.add_argument("-j", "--jobs", type=int, default=multiprocessing.cpu_count())
    p.add_argument("--top", type=int, default=20)
    p.set_defaults(func=cost)

    p = sub.add_parser("ya", help="add a ya make result")
    p.add_argument("--main", help="the main commit built (default: merge-base of HEAD and --main-ref)")
    p.add_argument("--main-ref", default="origin/main")
    p.add_argument("--kind", choices=("full", "incremental"), default="incremental")
    p.add_argument("--wall", type=float, required=True, help="seconds")
    p.add_argument("--cpu", type=float, help="seconds")
    p.add_argument("--note")
    p.set_defaults(func=ya_add)

    p = sub.add_parser("series", help="recorded runs by main commit next to ya make")
    p.add_argument("--label")
    p.add_argument("-o", "--output")
    p.set_defaults(func=series)

    p = sub.add_parser("baseline", help="TU durations of a build without PCH")
    p.add_argument("build_dir")
    p.add_argument("--replace", action="store_true", help="replace known durations too")
    p.set_defaults(func=baseline)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
