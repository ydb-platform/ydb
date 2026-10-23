#!/usr/bin/env python3
"""Checks that a build dir is fit for estimating precompiled headers
(//build/gn/pch) from its clang time traces -- a build of this source tree
with pch_mode = "none" and time_trace = true, up to date for --targets,
nothing building there, a trace and a .ninja_log entry for every C++ object
built -- then reads the traces: the time of the C++ compiles, how much of it
is parsing headers, the headers costing most and whether the PCH of
//build/gn/pch have them. Prints the problems found.

It then reads the history of main: the first-parent commits of the 4 weeks
before the merge base of HEAD and --main-ref, the files each one changed and
the generated files they change, and the C++ files each commit would rebuild
(their dependencies of .ninja_deps). Last, the classes of the C++ files
by their flags: a PCH serves only files compiled with the same flags but the
include dirs, warnings, forced includes, outputs and -D/-U (the -D of a PCH
are the ones all its files have).

Then each header worth a PCH compiled alone: one failing to is in a PCH
only with a header including it; the includes it shows complete the ones of
the traces. A header some file includes first needs no compile; the results
of earlier runs, at any revision, are kept in --cache-dir.

Then the groups of targets sharing a PCH and the headers of each PCH, in
each class: the headers adding most to G = q * (saving of a build from
scratch) + (1 - q) * (mean saving of a build of a commit with the cache of
its parent), where a commit changing a header of a PCH rebuilds all the files
of its group; the targets losing on their group leave it and form the next.

Last, the checks of each group: its PCH compiled with the flags of the
group, a file of each target compiled with it, -E with the -D only some of
its files have. A header breaking a target or depending on such a -D either
leaves the PCH (with the headers including it) or the targets it breaks
leave the group, whichever gives more G; the group is formed again and
checked again.

Then it writes //build/gn/pch for pch_mode = "global": groups.gni (the
targets, the generators of its headers, the flags of each group but the
targets reaching a generator, which would make a cycle) and a <name>/pch.h
per group, a group keeping the name of the one of the most targets in
common; the dirs of the groups gone are removed. Last, gn gen of the tree in
a temporary dir: each C++ file of the baseline has the PCH of its group and
the flags of its PCH.

Usage: pch_update.py --baseline <out> [--targets all tests]
                     [--max-missing-traces 0.01] [-j N] [--check-jobs N]
                     [--main-ref origin/main] [--cache-dir ~/.cache/pch_update]
Exit code: 0 if the build dir is fit and its traces and history have no
problems, else 1.
"""

import argparse
import array
import collections
import concurrent.futures
import hashlib
import heapq
import json
import multiprocessing
import os
import pickle
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import build_time_report as btr     # noqa: E402

_ROOT = os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
_ARG_RE = re.compile(r"^\s*([A-Za-z_]\w*)\s*=\s*(.*?)\s*$")
_STATUS_RE = re.compile(r"^\[(\d+)/(\d+)\] (.*)$")
# a trace is written right after its object; older than it by more is of another compile
_TRACE_SLACK_S = 60
_LIBCXX = "contrib/libs/cxxsupp/libcxx/include/"
_GEN = "<out>/gen/"
_UTIL = "//util:util"
_PCH_DIR = "build/gn/pch"
_INCLUDE_RE = re.compile(r'^\s*#\s*include\s*([<"])([^>"]+)[>"]')
_TOP_HEADERS = 20
_HISTORY_WEEKS = 4
_TOP_CHANGED = 10
_TOP_CLASSES = 10
_ENUM_OBJS = "_enum_objs"
_enum_exclude = None
# flags of a compile that don't make another flag class (the ones with a value
# as one token: -Ifoo, -Wfoo...; FLAG_ARGS are followed by their value)
_NOT_CLASS_PREFIXES = ("-I", "-W", "-D", "-U", "-ftime-trace")
_NOT_CLASS = {"<in>", "-c", "-MD", "-w"}
_INCLUDE_DIR_ARGS = ("-I", "-isystem", "-iquote", "-idirafter")
_FLAG_ARGS = {"-o", "-MF", "-I", "-isystem", "-iquote", "-idirafter", "-include", "-D", "-U", "-x"}
# what the checks drop of the command of a file (with a value: the next token)
_CHECK_DROP_ARGS = {"-o", "-MF", "-MT", "-MQ"}
_CHECK_DROP = {"-c", "-MD", "-MMD"}


def log(msg):
    sys.stderr.write("pch_update: %s\n" % msg)
    sys.stderr.flush()


# =============================================================================
# check
# =============================================================================

def running_ninja(build_dir):
    """Pids of ninja processes building in build_dir (as build_stats.py)."""
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


def read_args_gn(build_dir):
    """{name: value as written} of the one-line assignments of args.gn."""
    out = {}
    with open(os.path.join(build_dir, "args.gn"), encoding="utf-8") as f:
        for line in f:
            m = None if line.lstrip().startswith("#") else _ARG_RE.match(line)
            if m:
                out[m.group(1)] = m.group(2)
    return out


def read_ninja_log(build_dir):
    """(output -> (start_ms, end_ms) of its latest command, ninja runs, span
    of the last run in ms). The times are from the start of each run; a run
    logs its commands as they end, so an end earlier than the one before is a
    new run (a recompacted log, in no order, looks like many runs)."""
    latest, runs, prev_end, start = {}, 0, None, None
    with open(os.path.join(build_dir, ".ninja_log"), encoding="utf-8") as f:
        if not f.readline().startswith("# ninja log"):
            sys.exit("pch_update: unknown .ninja_log format")
        for line in f:
            fields = line.rstrip("\n").split("\t")
            if len(fields) < 4:
                continue
            s, e = int(fields[0]), int(fields[1])
            if prev_end is None or e < prev_end:
                runs += 1
                start = s
            start = min(start, s)
            prev_end = e
            latest[os.path.normpath(fields[3])] = (s, e)
    return latest, runs, (prev_end - start) if runs else 0


def dry_run(build_dir, targets):
    """(the descriptions of the commands ninja would run, stale manifest).
    -n runs nothing and writes neither .ninja_log nor .ninja_deps; with a
    stale manifest it stops after "Regenerating ninja files"."""
    env = dict(os.environ, NINJA_STATUS="[%f/%t] ")
    proc = subprocess.run(["ninja", "-C", build_dir, "-n"] + targets, env=env,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    if proc.returncode != 0:
        sys.exit("pch_update: ninja -n failed:\n%s" % proc.stdout[-4000:])
    out = [m.group(3) for m in map(_STATUS_RE.match, proc.stdout.splitlines()) if m]
    return out, "Regenerating ninja files" in out


def check(args):
    build_dir = os.path.realpath(args.baseline)
    errors, warnings = [], []

    if not os.path.exists(os.path.join(build_dir, "build.ninja")):
        sys.exit("pch_update: %s is not a gn build dir (no build.ninja)" % build_dir)
    source_root, _ = btr.detect_roots(build_dir)
    if source_root is None or os.path.realpath(source_root) != _ROOT:
        errors.append("a build of %s, not of %s" % (source_root and os.path.realpath(source_root), _ROOT))

    gn_args = read_args_gn(build_dir)
    pch_mode = gn_args.get("pch_mode", '"none"')
    if pch_mode != '"none"':
        errors.append("args.gn: pch_mode = %s, a build without PCH is needed (\"none\")" % pch_mode)
    if gn_args.get("time_trace") != "true":
        errors.append("args.gn: time_trace = %s, true is needed" % gn_args.get("time_trace", "false (default)"))

    busy = running_ninja(build_dir)
    if busy:
        errors.append("ninja is running there (pid %s)" % ", ".join(map(str, busy)))

    log("ninja -n %s..." % " ".join(args.targets))
    commands, stale = dry_run(build_dir, args.targets)
    if stale:
        errors.append("the ninja files are stale (gn files changed since the build): "
                      "ninja would regenerate them and rebuild what changed")
    to_compile = set(d[len("CXX "):] for d in commands if d.startswith("CXX "))

    log("reading the ninja manifest...")
    edges, rules = btr.parse_ninja_manifest(build_dir)
    cxx = [e for e in edges if e.rule == "cxx"]
    pch_edges = [e for e in edges if e.target.startswith("//build/gn/pch:")
                 or any(o.endswith((".pch", ".gch")) for o in e.outputs)]
    commands_of = dict((id(e), btr.edge_command(e, rules)) for e in cxx)
    with_pch = [e for e in cxx if "-include-pch" in commands_of[id(e)]]
    if pch_edges or with_pch:
        errors.append("the manifest has PCH: %d commands building one, %d C++ files using one"
                      % (len(pch_edges), len(with_pch)))
    other_toolchains = collections.Counter(e.rule for e in edges if e.rule.endswith("_cxx"))
    if other_toolchains:
        warnings.append("C++ files of other toolchains, not looked at: %s"
                        % ", ".join("%s %d" % kv for kv in sorted(other_toolchains.items())))

    log("looking at %d C++ objects and their traces..." % len(cxx))
    ninja_log, runs, span_ms = read_ninja_log(build_dir)
    no_flag, no_log, out_of_date = [], [], []
    not_built = collections.defaultdict(list)   # target -> objects
    no_trace = collections.defaultdict(list)    # target -> objects
    stale_traces = []
    built = 0
    trace_bytes = 0
    traced = []                                 # (object, trace, ninja ms)
    for e in cxx:
        obj = e.outputs[0]
        if "-ftime-trace" not in commands_of[id(e)]:
            no_flag.append(e)
        try:
            obj_mtime = os.stat(os.path.join(build_dir, obj)).st_mtime
        except OSError:
            not_built[e.target].append(obj)
            continue
        built += 1
        if obj in to_compile:
            out_of_date.append(obj)
        if obj not in ninja_log:
            no_log.append(obj)
        trace = btr.trace_path_for(build_dir, obj)
        if trace is None:
            no_trace[e.target].append(obj)
            continue
        st = os.stat(trace)
        trace_bytes += st.st_size
        if st.st_size == 0 or st.st_mtime < obj_mtime - _TRACE_SLACK_S:
            stale_traces.append(obj)
        elif obj in ninja_log:
            s, t = ninja_log[obj]
            traced.append((obj, trace, t - s))

    def by_target(objs, limit=10):
        top = sorted(objs.items(), key=lambda kv: (-len(kv[1]), kv[0]))[:limit]
        return "\n".join("      %-70s %d, e.g. %s" % (t, len(v), v[0]) for t, v in top)

    if out_of_date:
        errors.append("%d C++ objects out of date (ninja -n would compile them again), e.g. %s"
                      % (len(out_of_date), out_of_date[0]))
    if not_built:
        warnings.append("%d C++ files not built (failed, not reached or not in %s), left out, in %d targets:\n%s"
                        % (sum(len(v) for v in not_built.values()), " ".join(args.targets), len(not_built),
                           by_target(not_built)))
    others = len(commands) - len(to_compile) - stale
    if others and not stale:
        warnings.append("ninja -n would also run %d other commands (actions, links...) for %s"
                        % (others, " ".join(args.targets)))
    if no_flag:
        errors.append("%d C++ files compiled without -ftime-trace, e.g. %s (target %s)"
                      % (len(no_flag), no_flag[0].outputs[0], no_flag[0].target))
    if no_log:
        errors.append("%d C++ objects without a .ninja_log entry (no duration), e.g. %s"
                      % (len(no_log), no_log[0]))
    if stale_traces:
        errors.append("%d traces empty or older than their objects, e.g. %s" % (len(stale_traces), stale_traces[0]))
    missing = sum(len(v) for v in no_trace.values())
    if missing:
        text = "%d built C++ objects without a trace (%.2f%%) in %d targets:\n%s" % (
            missing, 100.0 * missing / max(built, 1), len(no_trace), by_target(no_trace))
        (errors if missing > args.max_missing_traces * built else warnings).append(text)
    if not os.path.exists(os.path.join(build_dir, ".ninja_deps")):
        warnings.append("no .ninja_deps: the header dependencies of the objects are unknown")

    head = subprocess.run(["git", "-C", _ROOT, "rev-parse", "--short", "HEAD"],
                          stdout=subprocess.PIPE, universal_newlines=True).stdout.strip()
    targets = set(e.target for e in cxx)
    print("baseline %s, source tree %s at %s" % (build_dir, _ROOT, head))
    print("  args.gn:          pch_mode = %s, time_trace = %s" % (pch_mode, gn_args.get("time_trace", "false")))
    print("  ninja -n:         %s" % (
        "the ninja files are stale" if stale else
        "%d commands to run for %s, %d of them C++" % (len(commands), " ".join(args.targets), len(to_compile))))
    print("  manifest:         %d targets, %d of them with C++: %d C++ files, with PCH %d" % (
        len(set(e.target for e in edges if e.target.startswith("//"))), len(targets), len(cxx), len(with_pch)))
    print("  built:            %d of %d C++ files; all files of %d of %d targets"
          % (built, len(cxx), len(targets) - len(not_built), len(targets)))
    print("  -ftime-trace:     %d of %d C++ files" % (len(cxx) - len(no_flag), len(cxx)))
    print("  traces:           %d of %d built (%.1f GB), empty or older than the object %d"
          % (built - missing, built, trace_bytes / 1e9, len(stale_traces)))
    print("  .ninja_log:       %d of %d built; %s" % (
        built - len(no_log), built,
        "1 ninja run of %.1f min" % (span_ms / 60000.0) if runs == 1 else
        "%d ninja runs, no build time" % runs))
    for w in warnings:
        print("WARNING: " + w)
    for e in errors:
        print("ERROR: " + e)
    return not errors, {"build_dir": build_dir, "edges": edges, "rules": rules, "traced": traced,
                        "ninja_log": ninja_log}


# =============================================================================
# traces
# =============================================================================

def pch_headers():
    """The headers of the PCH of //build/gn/pch now, as paths there."""
    pch_dir = os.path.join(_ROOT, _PCH_DIR)
    return sorted(os.path.join(d, f) for d in os.listdir(pch_dir) if os.path.isdir(os.path.join(pch_dir, d))
                  for f in os.listdir(os.path.join(pch_dir, d)) if f.endswith(".h"))


def pch_roots():
    """{include of a PCH of //build/gn/pch: (PCH header names)}; libc++
    headers spelled as the traces name them."""
    out = collections.defaultdict(set)
    for pch in pch_headers():
        with open(os.path.join(_ROOT, _PCH_DIR, pch), encoding="utf-8") as f:
            for line in f:
                m = _INCLUDE_RE.match(line)
                if m:
                    name = _LIBCXX + m.group(2) if m.group(1) == "<" else m.group(2)
                    out[name].add(pch)
    return dict((k, tuple(sorted(v))) for k, v in out.items())


def includes(name, spelling):
    """Whether `#include "spelling"` may be the header the traces name `name`:
    the include dir it is found in is unknown (the source root, gen/, the
    ADDINCL of a contrib library...)."""
    return name == spelling or name.endswith("/" + spelling)


def _init_trace_worker(paths, roots):
    global _roots, _pch_cache
    btr._init_worker(paths)
    _roots = roots
    _pch_cache = {}


def _pch_of(name):
    """The PCH whose includes `name` is one of."""
    out = _pch_cache.get(name)
    if out is None:
        out = set()
        for spelling, pch in _roots.items():
            if includes(name, spelling):
                out.update(pch)
        out = _pch_cache[name] = tuple(sorted(out))
    return out


def summarize_trace(job):
    """(object, summary or None, error) of one trace: its headers with their
    parse time (a header parsed twice: summed), the PCH each one is in (it is
    a header a PCH includes or nested in one), the parse time of the
    outermost headers, the header included first, ExecuteCompiler, Frontend,
    unbalanced Source events."""
    obj, data, err = btr.load_trace(job)
    if data is None:
        return obj, None, err
    names, ids, ts, dur = data["headers"]
    ids, ts, dur = array.array("i", ids), array.array("q", ts), array.array("q", dur)
    parse = [0] * len(names)
    own = [0] * len(names)      # parse time without the nested headers
    edges = set()               # (including, included) headers
    in_pch = {}
    outer = 0
    first = None                # the header the file includes first
    stack = []                  # (end, PCH of the header, header) of the open headers
    for i in sorted(range(len(ids)), key=lambda i: (ts[i], -dur[i])):
        while stack and ts[i] >= stack[-1][0]:
            stack.pop()
        h = ids[i]
        parse[h] += dur[i]
        own[h] += dur[i]
        pch = set(stack[-1][1]) if stack else set()
        pch.update(_pch_of(names[h]))
        if pch:
            in_pch[h] = tuple(sorted(pch.union(in_pch.get(h, ()))))
        if not stack:
            outer += dur[i]
            if first is None:
                first = h
        else:
            own[stack[-1][2]] -= dur[i]
            if stack[-1][2] != h:
                edges.add((stack[-1][2], h))
        stack.append((ts[i] + dur[i], tuple(pch), h))
    totals = data["totals"]
    return obj, {"names": names, "parse": parse, "own": own, "edges": sorted(edges),
                 "in_pch": in_pch, "outer": outer, "first": first,
                 "execute": totals.get("ExecuteCompiler", 0), "frontend": totals.get("Frontend", 0),
                 "unbalanced": data["source_unbalanced"]}, None


def compiler_dir(rules):
    """The install dir of the C++ compiler of the cxx rule, or None."""
    for token in rules.get("cxx", {}).get("command", "").split():
        if os.path.basename(token).startswith("clang++"):
            return os.path.dirname(os.path.dirname(os.path.realpath(token)))
    return None


def known_location(name, compiler):
    return not os.path.isabs(name) or bool(compiler and name.startswith(compiler + os.sep))


def display(name):
    if name.startswith(_LIBCXX):
        return "<%s>" % name[len(_LIBCXX):]
    if name.startswith("<out>/"):
        return name[len("<out>/"):]
    return name


def hours(us):
    h = us / 3.6e9
    return "%.1f h" % h if abs(h) >= 1 else "%.0f min" % (h * 60)


def duration(us):
    s = us / 1e6
    return "%.0f s" % s if abs(s) < 60 else "%.1f min" % (s / 60)


def traces(args, base):
    build_dir, traced = base["build_dir"], base["traced"]
    source_root, sysroot = btr.detect_roots(build_dir)
    paths = btr.Paths(build_dir, source_root, sysroot)
    compiler = compiler_dir(base["rules"])
    roots = pch_roots()
    ninja_ms = dict((obj, ms) for obj, _, ms in traced)

    log("reading .ninja_deps...")
    deps, dep_paths = btr.parse_ninja_deps(build_dir)
    dep_names = [paths.normalize(p) for p in dep_paths]
    objs = sorted(obj for obj, _, _ in traced)
    file_of = dict((obj, i) for i, obj in enumerate(objs))
    # the headers by id: the files parsing each one, its parse time there with
    # and without the nested headers; the includes seen in any file
    header_id, header_names = {}, []
    header_files, header_own, header_parse = [], [], []
    header_edges = set()
    base.update(paths=paths, deps=deps, dep_names=dep_names, compile_us={}, header_us={},
                objs=objs, header_names=header_names, header_files=header_files,
                header_own=header_own, header_parse=header_parse, header_edges=header_edges,
                first_header={})

    problems = collections.defaultdict(list)    # what -> examples
    parse = collections.Counter()               # header -> us over all files
    files = collections.Counter()               # header -> files
    in_pch = collections.defaultdict(set)
    execute = outer = 0
    read = 0
    log("reading %d traces with %d processes..." % (len(traced), args.jobs))
    pool = multiprocessing.Pool(args.jobs, initializer=_init_trace_worker, initargs=(paths, roots))
    jobs = [(obj, trace) for obj, trace, _ in traced]
    for n, (obj, t, err) in enumerate(pool.imap_unordered(summarize_trace, jobs, chunksize=8), 1):
        if n % 2000 == 0:
            log("%d of %d" % (n, len(jobs)))
        if t is None:
            problems["files: the trace is unreadable"].append("%s (%s)" % (obj, err))
            continue
        read += 1
        execute += t["execute"]
        base["compile_us"][obj] = t["execute"]
        base["header_us"][obj] = t["outer"]
        outer += t["outer"]
        names = t["names"]
        f = file_of[obj]
        ids = []
        for h, name in enumerate(names):
            parse[name] += t["parse"][h]
            files[name] += 1
            i = header_id.get(name)
            if i is None:
                i = header_id[name] = len(header_names)
                header_names.append(name)
                header_files.append(array.array("i"))
                header_own.append(array.array("q"))
                header_parse.append(array.array("q"))
            ids.append(i)
            header_files[i].append(f)
            header_own[i].append(max(t["own"][h], 0))
            header_parse[i].append(t["parse"][h])
        header_edges.update((ids[a] << 32) | ids[b] for a, b in t["edges"])
        if t["first"] is not None:
            base["first_header"][obj] = ids[t["first"]]
        for h, pch in t["in_pch"].items():
            in_pch[names[h]].update(pch)
        if t["unbalanced"]:
            problems["files: Source events without a pair"].append(obj)
        if t["outer"] > t["frontend"] * 1.01 + 1000 or t["frontend"] > t["execute"] * 1.01 + 1000:
            problems["files: headers longer than Frontend or Frontend longer than ExecuteCompiler"].append(obj)
        if t["execute"] / 1000.0 > ninja_ms[obj] * 1.05 + 100:
            problems["files: clang longer than the ninja command (a trace of another compile?)"].append(obj)
        own = set(dep_names[k] for k in deps.get(obj, ()))
        if not own:
            problems["files: no dependencies in .ninja_deps"].append(obj)
        elif not own.issuperset(names):
            problems["files: headers of the trace not among the dependencies of .ninja_deps"].append(
                "%s (%s)" % (obj, display(sorted(set(names) - own)[0])))
    pool.close()
    pool.join()
    for name in sorted(parse):
        if not known_location(name, compiler):
            problems["headers: neither in the source tree, the build dir, the sysroot nor the compiler"].append(name)
    missing = [display(r) for r in sorted(roots) if not any(includes(name, r) for name in parse)]
    if missing:
        problems["headers of //build/gn/pch in no trace"].extend(missing)

    print("traces:           %d of %d read" % (read, len(traced)))
    print("problems:         %d" % sum(len(v) for v in problems.values()))
    for what, examples in sorted(problems.items()):
        print("  %d %s, e.g. %s" % (len(examples), what, examples[0]))
    print("C++ compile:      %s, headers %s (%.0f%%)"
          % (hours(execute), hours(outer), 100.0 * outer / max(execute, 1)))
    print("top headers (parse, files, PCH):")
    for name, us in parse.most_common(_TOP_HEADERS):
        print("  %-60s %8s %7d   %s" % (display(name), hours(us), files[name],
                                       ", ".join(sorted(in_pch.get(name, ()))) or "-"))
    return not problems


# =============================================================================
# history
# =============================================================================

def git(*argv):
    proc = subprocess.run(["git", "-C", _ROOT] + list(argv), stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, universal_newlines=True)
    if proc.returncode != 0:
        sys.exit("pch_update: git %s: %s" % (" ".join(argv), proc.stderr.strip()))
    return proc.stdout


def commits_of(main_ref):
    """(merge base, its date, [(commit, [changed files])]) of the first-parent
    commits of main in _HISTORY_WEEKS weeks before the merge base of HEAD and
    main_ref; a merge as its diff to the first parent."""
    merge_base = git("merge-base", "HEAD", main_ref).strip()
    when = int(git("log", "-1", "--format=%ct", merge_base))
    out = git("log", "--first-parent", "-m", "--no-renames", "--name-only", "--format=%x00%H",
              "--since=%d" % (when - _HISTORY_WEEKS * 7 * 86400), merge_base)
    commits = []
    for chunk in out.split("\0")[1:]:
        lines = chunk.split("\n")
        commits.append((lines[0], [l for l in lines[1:] if l]))
    return merge_base, when, commits


def generated_sources(base):
    """{generated file as the dependencies name it: the source files it is
    made from}: the inputs of the command writing it and its depfile, through
    other generated files; not the build tools (the programs of the build dir)."""
    build_dir, paths = base["build_dir"], base["paths"]
    deps, dep_names = base["deps"], base["dep_names"]
    direct = {}                                 # generated -> [inputs]
    for e in base["edges"]:
        outs = [o for o in e.outputs if o.startswith("gen/")]
        if not outs or e.rule == "phony":
            continue
        ins = [paths.normalize(p) for p in e.inputs]
        for o in outs:
            ins_o = ins + [dep_names[k] for k in deps.get(o, ())]
            direct[paths.normalize(o)] = ins_o
    out = {}

    def sources(name, stack):
        if name in out:
            return out[name]
        if name in stack:
            return set()
        stack.add(name)
        found = set()
        for p in direct.get(name, ()):
            if p.startswith(_GEN):
                found |= sources(p, stack)
            elif not p.startswith("<") and not os.path.isabs(p):
                found.add(p)
        stack.discard(name)
        out[name] = found
        return found

    for name in direct:
        sources(name, set())
    return out


def history(args, base):
    build_dir = base["build_dir"]
    compile_us = base["compile_us"]
    deps, dep_names = base["deps"], base["dep_names"]
    problems = collections.defaultdict(list)

    log("reading the history of %s..." % args.main_ref)
    merge_base, when, commits = commits_of(args.main_ref)
    made_of = generated_sources(base)
    by_source = collections.defaultdict(set)    # source file -> generated files made of it
    for g, srcs in made_of.items():
        for f in srcs:
            by_source[f].add(g)

    # the dependencies of the C++ files: index by the files the window changes
    objs = base["objs"]
    changed = [set(files) for _, files in commits]
    for c in changed:
        for f in list(c):
            c.update(by_source.get(f, ()))
    touched = set().union(*changed) if changed else set()
    rdeps = collections.defaultdict(list)       # file -> indices of the C++ files depending on it
    for i, obj in enumerate(objs):
        names = set(dep_names[k] for k in deps.get(obj, ()))
        for name in names:
            if name.startswith(_GEN) and name not in made_of:
                problems["generated dependencies made by no command of the manifest"].append(name)
        for name in names & touched:
            rdeps[name].append(i)

    rebuilt, cost = [], []
    base.update(commit_files=changed, rebuilt=[])     # per commit: its files, the C++ files it rebuilds
    for c in changed:
        hit = set()
        for f in c:
            hit.update(rdeps.get(f, ()))
        base["rebuilt"].append(array.array("i", sorted(hit)))
        rebuilt.append(len(hit))
        cost.append(sum(compile_us[objs[i]] for i in hit))

    # a changed file of the commits with the generated files made of it: the
    # C++ files depending on them, and their compiles over its commits
    commits_of_file = collections.Counter(f for _, files in commits for f in set(files))
    dependents = {}
    for f in commits_of_file:
        hit = set(rdeps.get(f, ()))
        for g in by_source.get(f, ()):
            hit.update(rdeps.get(g, ()))
        if hit:
            dependents[f] = (len(hit), sum(compile_us[objs[i]] for i in hit))
    cost_of_file = collections.Counter(dict((f, commits_of_file[f] * us) for f, (_, us) in dependents.items()))
    reaching = set(f for c in changed for f in c if f in rdeps)
    generated = set(f for f in reaching if f.startswith(_GEN))

    def mean(v):
        return sum(v) / max(len(v), 1)

    def median(v):
        v = sorted(v)
        return v[len(v) // 2] if v else 0

    day = lambda t: time.strftime("%Y-%m-%d", time.localtime(t))
    print("history:          %d commits %s..%s (%s), empty %d" % (
        len(commits), day(when - _HISTORY_WEEKS * 7 * 86400), day(when), merge_base[:11],
        sum(1 for _, files in commits if not files)))
    print("problems:         %d" % sum(len(set(v)) for v in problems.values()))
    for what, examples in sorted(problems.items()):
        examples = sorted(set(examples))
        print("  %d %s, e.g. %s" % (len(examples), what, display(examples[0])))
    print("changed:          %d files, in C++ deps %d, generated %d" % (len(touched), len(reaching), len(generated)))
    print("rebuilt/commit:   median %d, mean %d (%s), none %d of %d" % (
        median(rebuilt), mean(rebuilt), hours(mean(cost)), sum(1 for n in rebuilt if not n), len(commits)))
    print("top changed (commits, dependents, compile):")
    for f, us in cost_of_file.most_common(_TOP_CHANGED):
        print("  %-60s %4d %6d %9s" % (f, commits_of_file[f], dependents[f][0], hours(us)))
    return not problems


# =============================================================================
# flag classes
# =============================================================================

def compile_flags(command, compiler):
    """(the flags making the class of a compile, its -D and -U, its include
    dirs as (option, dir), its forced includes as arguments) of a command
    of btr.edge_command; the wrapper before the compiler dropped."""
    command = command[command.index(compiler) + len(compiler):]
    argv = shlex.split(command) if any(c in command for c in "\"'\\") else command.split()
    flags, defines, include_dirs, forced = [], [], [], []
    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "-Xclang" and argv[i + 1:i + 2] == ["-include"] and argv[i + 2:i + 3] == ["-Xclang"]:
            forced.extend(argv[i:i + 4])        # a forced include: -Xclang -include -Xclang <file>
            i += 4
            continue
        if a in _FLAG_ARGS and i + 1 < len(argv):
            if a in ("-D", "-U"):
                defines.append(a + argv[i + 1])
            elif a in _INCLUDE_DIR_ARGS:
                include_dirs.append((a, argv[i + 1]))
            elif a == "-x":
                flags.append(a + " " + argv[i + 1])
            elif a == "-include":
                forced.extend(argv[i:i + 2])
            i += 2
            continue
        if a.startswith(("-D", "-U")):
            defines.append(a)
        elif a.startswith("-I"):
            include_dirs.append(("-I", a[2:]))
        elif a not in _NOT_CLASS and not a.startswith(_NOT_CLASS_PREFIXES):
            flags.append(a)
        i += 1
    return tuple(flags), defines, include_dirs, forced


def check_argv(command, compiler, source, obj):
    """The compile of a command of btr.edge_command as the checks run it: the
    wrapper, the output, the depfile and -ftime-trace dropped."""
    command = command[command.index(compiler):]
    argv = shlex.split(command) if any(c in command for c in "\"'\\") else command.split()
    out, i = [], 0
    while i < len(argv):
        a = argv[i]
        if a in _CHECK_DROP_ARGS:
            i += 2
            continue
        if a not in _CHECK_DROP and not a.startswith("-ftime-trace"):
            out.append(a.replace("<in>", source).replace("<out>", obj))
        i += 1
    return out


def pch_owner(label):
    """The target whose PCH the files of a target take: the one of its enum
    serialization for <target>_enum_objs (serialize_enums, //build/gn/base.gni)
    but for enum_serialization_pch_exclude."""
    global _enum_exclude
    d, _, name = label.partition(":")
    if not name.endswith(_ENUM_OBJS):
        return label
    owner = "%s:%s" % (d, name[:-len(_ENUM_OBJS)])
    if _enum_exclude is None:
        _enum_exclude = [p for _, ps in gni_lists(os.path.join(_ROOT, _TARGETS_GNI), "enum_serialization_pch_exclude")
                         for p in ps]
    return label if label_matches(owner, _enum_exclude) else owner


def flag_classes(args, base):
    compile_us, header_us = base["compile_us"], base["header_us"]
    rules = base["rules"]
    compiler = next(t for t in rules["cxx"]["command"].split() if os.path.basename(t).startswith("clang++"))
    by_class = collections.defaultdict(list)    # flags -> objects
    target_classes = collections.defaultdict(set)
    target_of, defines_of, include_dirs_of, source_of, forced_of, argv_of = {}, {}, {}, {}, {}, {}
    for e in base["edges"]:
        obj = e.outputs[0] if e.rule == "cxx" else None
        if obj not in compile_us:
            continue
        command = btr.edge_command(e, rules)
        flags, defines, include_dirs, forced = compile_flags(command, compiler)
        argv_of[obj] = check_argv(command, compiler, e.inputs[0], obj)
        by_class[flags].append(obj)
        target_classes[e.target].add(flags)
        target_of[obj] = pch_owner(e.target)
        defines_of[obj] = defines
        include_dirs_of[obj] = include_dirs
        source_of[obj] = e.inputs[0]
        forced_of[obj] = forced

    classes = sorted(by_class.items(), key=lambda kv: -sum(compile_us[o] for o in kv[1]))
    base.update(classes=[objs for _, objs in classes], class_flags=[flags for flags, _ in classes],
                target_of=target_of, defines_of=defines_of, include_dirs_of=include_dirs_of,
                source_of=source_of, forced_of=forced_of, argv_of=argv_of, compiler=compiler,
                class_of=dict((o, n) for n, (_, objs) in enumerate(classes, 1) for o in objs))
    split = sorted(t for t, c in target_classes.items() if len(c) > 1)
    first = classes[0][0] if classes else ()
    print("flag classes:     %d of %d C++ files, targets in several classes %d%s" % (
        len(classes), sum(len(v) for _, v in classes), len(split), (", e.g. " + split[0]) if split else ""))
    print("  %-3s %7s %7s %9s %9s %11s  %s" % ("#", "targets", "files", "compile", "headers", "-D common", "flags"))
    rest = [0, 0, 0, 0]
    for n, (flags, objs) in enumerate(classes, 1):
        row = [len(set(target_of[o] for o in objs)), len(objs),
               sum(compile_us[o] for o in objs), sum(header_us[o] for o in objs)]
        if n > _TOP_CLASSES:
            rest = [a + b for a, b in zip(rest, row)]
            continue
        common = set(defines_of[objs[0]])
        for o in objs[1:]:
            common.intersection_update(defines_of[o])
        names = set(d.partition("=")[0] for o in objs for d in defines_of[o])
        diff = ["+" + f for f in flags if f not in first] + ["-" + f for f in first if f not in flags]
        print("  %-3d %7d %7d %9s %9s %5d/%-5d  %s" % (n, row[0], row[1], hours(row[2]), hours(row[3]),
                                                   len(common), len(names), " ".join(diff) or "-"))
    if len(classes) > _TOP_CLASSES:
        print("  %-3s %7d %7d %9s %9s" % ("...", rest[0], rest[1], hours(rest[2]), hours(rest[3])))
    return not split


# =============================================================================
# headers alone
# =============================================================================

_HEADER_MIN_PARSE = 30 * 10**6  # us of parse in all the files: the headers parsed less are not checked alone
_SHOW_H_RE = re.compile(r"^(\.+) (.*)$")
_COMMENT_RE = re.compile(r"/\*.*?\*/|//[^\n]*", re.S)
_INCLUDE_LINE_RE = re.compile(r"^\s*#\s*include\b")
_SOURCE_PREFIX = 1 << 16        # bytes of a source read for what comes before its first #include
_CACHE_VARIANTS = 8             # results kept per header
_TREE_SHIFT, _TREE_ID = 40, (1 << 40) - 1


def includes_first(path):
    """Whether only comments come before the first #include of a source."""
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            text = _COMMENT_RE.sub(" ", f.read(_SOURCE_PREFIX))
    except OSError:
        return False
    for line in text.splitlines():
        if line.strip():
            return bool(_INCLUDE_LINE_RE.match(line))
    return False


class FactsCache(object):
    """The headers compiled alone by earlier runs, at any revision: per
    header up to _CACHE_VARIANTS results, each valid while the command (paths
    of the tree and build dir taken out) and every file its -H shows have the
    same content."""

    def __init__(self, path, base):
        self.path, self.base = path, base
        self.names, self.stats, self.headers = [], {}, {}
        try:
            with open(path, "rb") as f:
                self.names, self.stats, self.headers = pickle.load(f)
        except (OSError, EOFError, pickle.UnpicklingError, ValueError):
            pass
        self.name_id = dict((n, i) for i, n in enumerate(self.names))
        self.digests = {}
        prefixes = []
        for r in base["paths"].source_roots:
            prefixes += [(r, "<root>"), (os.path.relpath(r, base["build_dir"]), "<root>")]
        prefixes.append((base["build_dir"], "<out>"))
        self.prefixes = sorted(prefixes, key=lambda p: -len(p[0]))

    def key(self, argv):
        out = []
        for a in argv:
            for p, mark in self.prefixes:
                a = a.replace(p, mark)
            out.append(a)
        return hashlib.sha1("\0".join(out).encode()).hexdigest()

    def digest(self, name):
        d = self.digests.get(name)
        if d is None:
            path = real_path(self.base, name)
            try:
                st = os.stat(path)
                known = self.stats.get(path)
                if known and known[:2] == (st.st_size, st.st_mtime_ns):
                    d = known[2]
                else:
                    with open(path, "rb") as f:
                        d = hashlib.sha1(f.read()).digest()
                    self.stats[path] = (st.st_size, st.st_mtime_ns, d)
            except OSError:
                d = b""
            self.digests[name] = d
        return d

    def content(self, tree):
        h = hashlib.sha1()
        for x in tree:
            h.update(self.digest(self.names[x & _TREE_ID]))
        return h.digest()

    def get(self, header, key):
        """(returncode, -H tree as [(depth, name)], error) or None."""
        variants = self.headers.get(header, [])
        for i, (k, tree, content, rc, error) in enumerate(variants):
            if k == key and self.content(tree) == content:
                variants.insert(0, variants.pop(i))
                return rc, [(x >> _TREE_SHIFT, self.names[x & _TREE_ID]) for x in tree], error
        return None

    def put(self, header, key, rc, tree, error):
        ids = array.array("q")
        for depth, name in tree:
            i = self.name_id.get(name)
            if i is None:
                i = self.name_id[name] = len(self.names)
                self.names.append(name)
            ids.append(depth << _TREE_SHIFT | i)
        variants = self.headers.setdefault(header, [])
        variants.insert(0, (key, ids, self.content(ids), rc, error))
        del variants[_CACHE_VARIANTS:]

    def save(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        tmp = self.path + ".%d.tmp" % os.getpid()
        with open(tmp, "wb") as f:
            pickle.dump((self.names, self.stats, self.headers), f, protocol=4)
        os.replace(tmp, self.path)


def header_facts(args, base):
    """Compiles each header worth a PCH alone, with the flags of a file
    parsing it. A header failing to, or not checked, can be in a PCH only
    with a header including it, not as a root. The includes shown complete
    the ones of the traces: there a header the file included before is not
    seen again. A header a file of its class includes first, with no forced
    includes and only comments before, compiles alone as its trace shows,
    with all its includes; the rest come from the cache of earlier runs or
    are compiled."""
    names, objs, class_of = base["header_names"], base["objs"], base["class_of"]
    header_id = dict((name, h) for h, name in enumerate(names))
    normalize = base["paths"].normalize
    parents = collections.defaultdict(set)
    mask = (1 << 32) - 1
    for x in base["header_edges"]:
        parents[x & mask].add(x >> 32)
    candidates = [h for h in range(len(names))
                  if len(base["header_files"][h]) >= 2 and sum(base["header_parse"][h]) >= _HEADER_MIN_PARSE]
    first_of = collections.defaultdict(list)
    for obj, h in base["first_header"].items():
        first_of[h].append(obj)
    cache = FactsCache(os.path.join(args.cache_dir, "header_facts.pickle"), base)

    ok, trees, errors = set(), {}, {}
    from_traces = from_cache = 0
    tmp = tempfile.mkdtemp(prefix="pch_update.")
    try:
        jobs, keys = [], {}
        for h in candidates:
            files = base["header_files"][h]
            n = collections.Counter(class_of[objs[t]] for t in files).most_common(1)[0][0]
            # a header a file includes first compiles alone; the trace misses
            # the includes shorter than its granularity: -E -H shows them
            traced = any(class_of[o] == n and not base["forced_of"][o] and
                         includes_first(os.path.join(base["build_dir"], base["source_of"][o])) for o in first_of[h])
            if traced:
                ok.add(h)
                from_traces += 1
            obj = min(objs[t] for t in files if class_of[objs[t]] == n)
            argv = ([base["compiler"]] + list(base["class_flags"][n - 1]) + base["defines_of"][obj] +
                    [a for opt, d in base["include_dirs_of"][obj] for a in (opt, d)] +
                    ["-w", "-E" if traced else "-fsyntax-only", "-H", "-ferror-limit=1"])
            keys[h] = key = cache.key(argv + [spelling(base, names[h])])
            got = cache.get(names[h], key)
            if got is not None:
                rc, trees[h], errors[h] = got
                if not rc:
                    ok.add(h)
                from_cache += not traced
                continue
            src = os.path.join(tmp, "h%d.cpp" % h)
            with open(src, "w") as f:
                f.write("#include %s\n" % spelling(base, names[h]))
            jobs.append(("H", h, argv + [src], base["build_dir"]))
        log("compiling %d headers alone with %d processes..." % (len(jobs), args.check_jobs))
        with concurrent.futures.ProcessPoolExecutor(args.check_jobs) as executor:
            for i, (_, h, (rc, _, err, _)) in enumerate(executor.map(_run, jobs, chunksize=4), 1):
                tree, error = [], None
                for line in err.splitlines():
                    mh = _SHOW_H_RE.match(line)
                    if mh:
                        tree.append((len(mh.group(1)), normalize(mh.group(2))))
                        continue
                    me = _ERROR_RE.match(line)
                    if rc and me and error is None:
                        error = "%s:%s: %s" % (display(normalize(me.group(1))), me.group(2), me.group(3))
                trees[h], errors[h] = tree, error
                if not rc:
                    ok.add(h)
                cache.put(names[h], keys[h], rc, tree, error)
                if i % 500 == 0:
                    log("%d of %d" % (i, len(jobs)))
        cache.save()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    learned, example = 0, None
    for h, tree in sorted(trees.items()):
        if h not in ok and example is None and errors[h]:
            example = "%s: %s" % (display(names[h]), errors[h])
        stack = [None]
        for depth, name in tree:
            name = header_id.get(name)
            del stack[depth:]
            parent = stack[-1] if len(stack) == depth else None
            stack.extend([None] * (depth - len(stack)) + [name])
            if parent is not None and name is not None and parent != name and parent not in parents[name]:
                parents[name].add(parent)
                base["header_edges"].add(parent << 32 | name)
                learned += 1
    base["self_contained"] = ok
    print("headers alone:    %d of %d compile (parsed %s+ in 2+ files): from traces %d (includes by -E), from cache %d, "
          "compiled %d; includes learned %d%s" % (
              len(ok), len(candidates), duration(_HEADER_MIN_PARSE), from_traces, from_cache,
              len(candidates) - from_traces - from_cache, learned, ("; e.g. " + example) if example else ""))
    print("  the rest in a PCH only with a header including them: %d of %d" % (len(names) - len(ok), len(names)))
    return True


# =============================================================================
# groups
# =============================================================================

_Q = 0.1                # share of the CI builds with an empty cache
_CORES = 52             # of a CI machine
_LOAD_SHARE = 0.02      # a file loading a PCH spends this share of the PCH compile: a guess until measured
_MAX_GROUPS = 8         # per class
_SPLIT_SEEDS = 3        # headers tried for splitting a group
_SEED_CANDIDATES = 300  # headers of the most parse the seeds are taken from
_PACKAGES = 30          # headers tried with the headers they include at once
_MEMBERSHIP_ROUNDS = 5
_TOP_RESIDUAL = 15
_TOP_DIRS = 4
_TOP_ROOTS = 3
_REASONS = ("files without PCH", "changes often", "few files for the load cost", "includes such headers",
            "dropped by the checks")


def strong_components(children):
    """(the component of each node, their number); a component is numbered
    after the ones it reaches (Tarjan)."""
    n = len(children)
    index, low, comp = [-1] * n, [0] * n, [-1] * n
    on_stack = bytearray(n)
    stack, counter, count = [], 0, 0
    for root in range(n):
        if index[root] != -1:
            continue
        index[root] = low[root] = counter
        counter += 1
        stack.append(root)
        on_stack[root] = 1
        work = [(root, 0)]
        while work:
            v, i = work[-1]
            if i < len(children[v]):
                work[-1] = (v, i + 1)
                w = children[v][i]
                if index[w] == -1:
                    index[w] = low[w] = counter
                    counter += 1
                    stack.append(w)
                    on_stack[w] = 1
                    work.append((w, 0))
                elif on_stack[w]:
                    low[v] = min(low[v], index[w])
                continue
            work.pop()
            if work:
                u = work[-1][0]
                low[u] = min(low[u], low[v])
            if low[v] == index[v]:
                w = -1
                while w != v:
                    w = stack.pop()
                    on_stack[w] = 0
                    comp[w] = count
                count += 1
    return comp, count


def ready_times(base):
    """{generated file as the traces name it: us its command ends at with
    unlimited parallelism, the durations of .ninja_log}."""
    edges, ninja_log, paths = base["edges"], base["ninja_log"], base["paths"]
    durations = []
    for e in edges:
        se = ninja_log.get(os.path.normpath(e.outputs[0])) if e.outputs else None
        durations.append(se[1] - se[0] if se else 0)
    _, finish, _ = btr.critical_path(edges, durations)
    out = {}
    for i, e in enumerate(edges):
        for o in e.outputs:
            if o.startswith("gen/"):
                out[paths.normalize(o)] = finish[i] * 1000
    return out


def build_model(base):
    """The headers as units -- a header or the headers including each other
    -- with what a PCH having a unit costs and saves; the files and commits."""
    names = base["header_names"]
    n = len(names)
    mask = (1 << 32) - 1
    children = [[] for _ in range(n)]
    for x in base["header_edges"]:
        children[x >> 32].append(x & mask)
    comp, count = strong_components(children)
    headers = [[] for _ in range(count)]
    for h, u in enumerate(comp):
        headers[u].append(h)
    unit_children = [set() for _ in range(count)]
    for x in base["header_edges"]:
        a, b = comp[x >> 32], comp[x & mask]
        if a != b:
            unit_children[a].add(b)
    unit_parents = [[] for _ in range(count)]
    for u, cs in enumerate(unit_children):
        for v in cs:
            unit_parents[v].append(u)

    files, own, parse = [], [], []
    for u in range(count):
        hs = headers[u]
        if len(hs) == 1:
            h = hs[0]
            files.append(base["header_files"][h])
            own.append(base["header_own"][h])
            parse.append(base["header_parse"][h])
            continue
        merged = collections.defaultdict(lambda: [0, 0])
        for h in hs:
            for f, o, p in zip(base["header_files"][h], base["header_own"][h], base["header_parse"][h]):
                merged[f][0] += o
                merged[f][1] = max(merged[f][1], p)
        fs = sorted(merged)
        files.append(array.array("i", fs))
        own.append(array.array("q", (merged[f][0] for f in fs)))
        parse.append(array.array("q", (merged[f][1] for f in fs)))

    def median(v):
        return sorted(v)[len(v) // 2] if v else 0

    build = [sum(median(base["header_own"][h]) for h in hs) for hs in headers]
    total_parse = [sum(p) for p in parse]

    header_id = dict((name, h) for h, name in enumerate(names))
    commits = [set() for _ in range(count)]
    for k, changed in enumerate(base["commit_files"]):
        for f in changed:
            h = header_id.get(f)
            if h is not None:
                commits[comp[h]].add(k)
    if "ready_of" not in base:
        base["ready_of"] = ready_times(base)
    ready_of = base["ready_of"]
    ready = [max([ready_of.get(names[h], 0) for h in hs]) for hs in headers]

    objs = base["objs"]
    nfiles = len(objs)
    ntotal = array.array("i", [0] * nfiles)
    for rebuilt in base["rebuilt"]:
        for t in rebuilt:
            ntotal[t] += 1
    target_of = base["target_of"]
    files_of = collections.defaultdict(list)
    for t, obj in enumerate(objs):
        files_of[target_of[obj]].append(t)
    # a unit can be a root of a PCH if a header of it compiles alone
    ok = base.get("self_contained")
    rootable = bytearray(1 if ok is None or any(h in ok for h in hs) else 0 for hs in headers)
    return {"comp": comp, "headers": headers, "children": unit_children, "parents": unit_parents,
            "rootable": rootable,
            "files": files, "own": own, "parse": parse, "build": build, "total_parse": total_parse,
            "commits": commits, "ready": ready, "nfiles": nfiles, "ntotal": ntotal,
            "compile": [base["compile_us"][obj] for obj in objs], "rebuilt": base["rebuilt"],
            "ncommits": len(base["rebuilt"]), "files_of": files_of}


class Selection(object):
    """The headers of the PCH of a group of files: greedily the units adding
    most to G, each after the units it includes; then a unit with all it
    includes at once, for the ones held back by what they include. A unit
    that cannot be a root comes only with a unit including it."""

    def __init__(self, m, members, banned=None):
        self.m, self.members = m, members
        self.banned = banned or bytearray(len(m["headers"]))
        c, nfiles = m["compile"], m["nfiles"]
        self.cons = cons = bytearray(nfiles)
        for target in members:
            for t in m["files_of"][target]:
                cons[t] = 1
        self.n_cons = sum(cons)
        self.c_cons = sum(c[t] for t in range(nfiles) if cons[t])
        ncommits = m["ncommits"]
        self.rebuilt_c, self.rebuilt_n = [0] * ncommits, [0] * ncommits
        for k, rebuilt in enumerate(m["rebuilt"]):
            a = r = 0
            for t in rebuilt:
                if cons[t]:
                    a += c[t]
                    r += 1
            self.rebuilt_c[k], self.rebuilt_n[k] = a, r
        self.nonhit = array.array("i", m["ntotal"])
        self.p = array.array("q", [0] * nfiles)
        self.parse = 0                          # of the PCH headers in the files of the group
        self.build = 0
        self.hit = set()                        # commits changing a PCH header
        self.hit_c = 0
        self.nonhit_p = 0                       # sum of p over the files the commits not in hit rebuild
        self.nonhit_n = sum(self.rebuilt_n)
        self.ready = 0
        self.selected = bytearray(len(m["headers"]))
        rootable = m["rootable"]
        self.pending = [sum(1 for v in cs if rootable[v]) for cs in m["children"]]
        self.s1, self.s2, self.s2_epoch, self.pot = {}, {}, {}, {}
        self.hit_epoch = self.p_epoch = 0
        self.pk = {}
        self.value = 0.0
        self.run()

    def evaluate(self, parse, build, hit, hit_c, nonhit_p, nonhit_n, ready):
        """(G, from scratch, warm per build) in us of CPU per build."""
        saved = parse - _LOAD_SHARE * build * self.n_cons
        cold = saved - build - _CORES * (ready + build)
        warm = (hit * (saved - self.c_cons - build) + hit_c + nonhit_p
                - _LOAD_SHARE * build * nonhit_n) / max(self.m["ncommits"], 1)
        return _Q * cold + (1 - _Q) * warm, cold, warm

    def sums(self, u):
        if u not in self.s1 or self.s2_epoch[u] != self.hit_epoch:
            cons, nonhit = self.cons, self.nonhit
            s1 = s2 = pot = 0
            for t, e, p in zip(self.m["files"][u], self.m["own"][u], self.m["parse"][u]):
                if cons[t]:
                    s1 += e
                    s2 += e * nonhit[t]
                    pot += p
            self.s1[u], self.s2[u], self.pot[u], self.s2_epoch[u] = s1, s2, pot, self.hit_epoch
        return self.s1[u], self.s2[u]

    def commit_p(self, k):
        if self.pk.get(k, (None, -1))[1] != self.p_epoch:
            cons, p = self.cons, self.p
            self.pk[k] = (sum(p[t] for t in self.m["rebuilt"][k] if cons[t]), self.p_epoch)
        return self.pk[k][0]

    def gain(self, units, with_commits=True):
        m = self.m
        s1 = s2 = build = 0
        ready = self.ready
        new = set()
        for v in units:
            a, b = self.sums(v)
            s1 += a
            s2 += b
            build += m["build"][v]
            ready = max(ready, m["ready"][v])
            if with_commits:
                new.update(m["commits"][v])
        new -= self.hit
        g, _, _ = self.evaluate(
            self.parse + s1, self.build + build, len(self.hit) + len(new),
            self.hit_c + sum(self.rebuilt_c[k] for k in new),
            self.nonhit_p - sum(self.commit_p(k) for k in new) + s2 - min(s2, len(new) * s1),
            self.nonhit_n - sum(self.rebuilt_n[k] for k in new), ready)
        return g - self.value

    def accept(self, units):
        m, cons, nonhit, p = self.m, self.cons, self.nonhit, self.p
        new = set()
        for v in units:
            new.update(m["commits"][v])
        new -= self.hit
        old_p = sum(self.commit_p(k) for k in new)
        for k in new:
            for t in m["rebuilt"][k]:
                if cons[t]:
                    nonhit[t] -= 1
            self.hit_c += self.rebuilt_c[k]
            self.nonhit_n -= self.rebuilt_n[k]
        if new:
            self.hit.update(new)
            self.hit_epoch += 1
        added = 0
        eligible = []
        for v in units:
            s1, _ = self.sums(v)
            self.parse += s1
            self.build += m["build"][v]
            self.ready = max(self.ready, m["ready"][v])
            for t, e in zip(m["files"][v], m["own"][v]):
                p[t] += e
                if cons[t]:
                    added += e * nonhit[t]
            self.selected[v] = 1
        for v in units:
            if not m["rootable"][v]:
                continue
            for u in m["parents"][v]:
                self.pending[u] -= 1
                if not self.pending[u] and not self.selected[u]:
                    eligible.append(u)
        self.nonhit_p += added - old_p
        self.p_epoch += 1
        self.value = self.evaluate(self.parse, self.build, len(self.hit), self.hit_c,
                                   self.nonhit_p, self.nonhit_n, self.ready)[0]
        return eligible

    def closure(self, u):
        seen, stack = set([u]), [u]
        while stack:
            for v in self.m["children"][stack.pop()]:
                if v not in seen and not self.selected[v]:
                    seen.add(v)
                    stack.append(v)
        return seen

    def with_children(self, u):
        """u with the units it includes that cannot be a root and are not
        selected yet, and what they include."""
        rootable = self.m["rootable"]
        if all(rootable[v] or self.selected[v] for v in self.m["children"][u]):
            return [u]
        return list(self.closure(u))

    def run(self):
        m = self.m
        rootable = m["rootable"]
        heap = []

        def push(units):
            for u in units:
                if rootable[u] and not self.banned[u] and self.sums(u)[0] > 0:
                    heapq.heappush(heap, (-self.gain(self.with_children(u)), u, self.p_epoch))

        push(u for u in range(len(m["headers"])) if not self.pending[u])
        candidates = sorted(range(len(m["headers"])), key=lambda u: -m["total_parse"][u])[:_SEED_CANDIDATES]
        while True:
            while heap:
                g, u, epoch = heapq.heappop(heap)
                if self.selected[u]:
                    continue
                if epoch != self.p_epoch:
                    heapq.heappush(heap, (-self.gain(self.with_children(u)), u, self.p_epoch))
                    continue
                if -g <= 0:
                    heapq.heappush(heap, (g, u, epoch))
                    break
                push(self.accept(self.with_children(u)))
            best, best_units = 0, None
            open_ = [u for u in candidates if not self.selected[u] and not self.banned[u] and rootable[u]]
            for u in open_:
                self.sums(u)
            for u in sorted(open_, key=lambda u: -self.pot[u])[:_PACKAGES]:
                units = self.closure(u)
                g = self.gain(units)
                if g > best:
                    best, best_units = g, units
            if best_units is None:
                break
            push(self.accept(best_units))

    def contributions(self, targets):
        """{target: what it adds to G}."""
        m = self.m
        hits = collections.Counter()
        for k in self.hit:
            hits.update(m["rebuilt"][k])
        ncommits, nhit = max(m["ncommits"], 1), len(self.hit)
        load = _LOAD_SHARE * self.build
        out = {}
        for target in targets:
            files = m["files_of"][target]
            total = 0.0
            for t in files:
                s = self.p[t] - load
                h = hits.get(t, 0)
                total += _Q * s + (1 - _Q) * (s * (nhit + m["ntotal"][t] - h) - m["compile"][t] * (nhit - h)) / ncommits
            out[target] = total
        return out

    def reason(self, u):
        """Why a unit with parse in the files of the group is not in its PCH."""
        if self.banned[u]:
            return 4
        if self.m["commits"][u] - self.hit and self.gain([u], with_commits=False) > 0:
            return 1
        if self.gain([u], with_commits=False) <= 0:
            return 2
        return 3


def banned_units(m, units):
    """The units with all the units including them: a PCH without a unit can
    have none of them."""
    out = bytearray(len(m["headers"]))
    stack = list(units)
    while stack:
        u = stack.pop()
        if not out[u]:
            out[u] = 1
            stack.extend(m["parents"][u])
    return out


def form_group(m, pool, banned=None):
    """A selection for the targets of `pool` gaining from it: the targets
    adding to G stay, the selection is made again for them."""
    members = set(pool)
    sel = None
    for _ in range(_MEMBERSHIP_ROUNDS):
        sel = Selection(m, members, banned)
        contrib = sel.contributions(pool)
        new = set(t for t in pool if contrib.get(t, 0) > 0)
        if new == members or not new:
            break
        members = new
    return sel


def split_seeds(m, sel):
    """The units of the most parse in the files of `sel` not in its PCH."""
    cons = sel.cons
    pot = {}
    for u in sorted(range(len(m["headers"])), key=lambda u: -m["total_parse"][u])[:_SEED_CANDIDATES]:
        if not sel.selected[u]:
            pot[u] = sum(p for t, p in zip(m["files"][u], m["parse"][u]) if cons[t])
    return sorted(pot, key=lambda u: -pot[u])[:_SPLIT_SEEDS]


class _Taken(Selection):
    """A selection of the units given to accept()."""

    def run(self):
        pass


def shrunk(m, pool, headers, banned):
    """A selection for the targets of `pool` gaining from it, of the units of
    `headers` having no other header, none banned: no header added."""
    units = set(m["comp"][h] for h in headers)
    units = [u for u in units if not banned[u] and all(h in headers for h in m["headers"][u])]
    members = set(pool)
    sel = None
    for _ in range(_MEMBERSHIP_ROUNDS):
        sel = _Taken(m, members, banned)
        sel.accept(units)
        contrib = sel.contributions(pool)
        new = set(t for t in pool if contrib.get(t, 0) > 0)
        if new == members or not new:
            break
        members = new
    return sel


def groups_of_class(m, targets, banned=None):
    pool, out = set(targets), []
    while pool and len(out) < _MAX_GROUPS:
        sel = form_group(m, pool, banned)
        if sel.value <= 0:
            break
        out.append(sel)
        pool -= sel.members
    i = 0
    while i < len(out) and len(out) < _MAX_GROUPS:
        sel, best = out[i], None
        for u in split_seeds(m, sel):
            files = set(m["files"][u])
            sub = set(t for t in sel.members
                      if 2 * sum(1 for f in m["files_of"][t] if f in files) >= len(m["files_of"][t]))
            if not sub or sub == sel.members:
                continue
            parts = [form_group(m, sub, banned), form_group(m, sel.members - sub, banned)]
            value = sum(p.value for p in parts if p.value > 0)
            if value > sel.value and (best is None or value > best[0]):
                best = (value, [p for p in parts if p.value > 0])
        if best:
            out[i:i + 1] = best[1]
        else:
            i += 1
    return out


def no_util(base, m):
    """The contrib part: the targets not depending on //util (NO_UTIL), and
    the headers they cannot have -- the ones of only targets depending on
    //util (owners: the targets of the header's dir and the dirs above it,
    the producer of a generated one), parsed in no file of the part."""
    producer, deps, exe = target_graph(base)
    util = {}

    def has_util(t):
        if t not in util:
            util[t] = t == _UTIL or _UTIL in reachable(deps, exe, [t])
        return util[t]

    dir_targets = collections.defaultdict(set)
    for t in set(producer.values()) | set(deps):
        if t and t.startswith("//"):
            dir_targets[t[2:].partition(":")[0]].add(t)

    def util_world(name):
        if name.startswith(_LIBCXX) or name.startswith("<sysroot>") or os.path.isabs(name):
            return False
        owners = set()
        if name.startswith("<out>/"):
            name = name[len("<out>/"):]
            if name in producer:
                owners.add(producer[name])
            name = name[len("gen/"):] if name.startswith("gen/") else name
        d = os.path.dirname(name)
        while d:
            owners |= dir_targets.get(d, set())
            d = os.path.dirname(d)
        return bool(owners) and all(has_util(o) for o in owners)

    contrib = set(t for t in m["files_of"] if not t.endswith(_ENUM_OBJS) and not has_util(t))
    files = set(f for t in contrib for f in m["files_of"][t])
    names = base["header_names"]
    banned = set(h for u, hs in enumerate(m["headers"]) if not files.intersection(m["files"][u])
                 for h in hs if util_world(names[h]))
    return contrib, banned


def is_contrib(base, sel):
    return bool(sel.members) and sel.members <= base["contrib"]


def groups(args, base):
    log("forming groups...")
    m = build_model(base)
    target_of = base["target_of"]
    contrib, contrib_bans = no_util(base, m)
    base.update(contrib=contrib, contrib_bans=contrib_bans)
    banned = banned_units(m, set(m["comp"][h] for h in contrib_bans))
    print("%-18s%d of %d targets without //util, headers banned for them %d" % (
        "contrib:", len(contrib), sum(1 for t in m["files_of"] if not t.endswith(_ENUM_OBJS)), len(contrib_bans)))

    found = []                                  # (class, selection)
    for n, class_objs in enumerate(base["classes"], 1):
        # the enum serialization of enum_serialization_pch_exclude: no PCH
        targets = sorted(set(target_of[o] for o in class_objs if not target_of[o].endswith(_ENUM_OBJS)))
        log("class %d: %d targets..." % (n, len(targets)))
        found.extend((n, sel) for sel in groups_of_class(m, [t for t in targets if t not in contrib]))
        # contrib: one group, no subgroups
        own = [t for t in targets if t in contrib]
        if own:
            sel = form_group(m, own, banned)
            if sel.value > 0:
                found.append((n, sel))
    base.update(model=m, found=found)
    report_groups(base, "groups")
    return True


def selection_roots(base, m, sel):
    """The headers a PCH includes: of the units no selected unit includes,
    the headers compiling alone."""
    ok = base.get("self_contained")
    return [h for u in range(len(m["headers"])) if sel.selected[u] and not any(sel.selected[q] for q in m["parents"][u])
            for h in m["headers"][u] if ok is None or h in ok]


def report_groups(base, title):
    m, found = base["model"], base["found"]
    names, comp = base["header_names"], m["comp"]
    group_of = [None] * m["nfiles"]
    for g, (_, sel) in enumerate(found):
        for target in sel.members:
            for t in m["files_of"][target]:
                group_of[t] = g

    # the parse not in PCH: by reason; by header, as its parse with the nested ones
    by_reason = [0] * len(_REASONS)
    covered = 0
    residual = collections.Counter()
    residual_files = collections.Counter()
    residual_reason = collections.defaultdict(collections.Counter)
    reasons = [dict() for _ in found]
    for h in range(len(names)):
        u = comp[h]
        for t, own, parse in zip(base["header_files"][h], base["header_own"][h], base["header_parse"][h]):
            g = group_of[t]
            if g is not None and found[g][1].selected[u]:
                covered += own
                continue
            if g is None:
                r = 0
            else:
                r = reasons[g].get(u)
                if r is None:
                    r = reasons[g][u] = found[g][1].reason(u)
            by_reason[r] += own
            residual[h] += parse
            residual_files[h] += 1
            residual_reason[h][_REASONS[r]] += parse

    total_parse = sum(base["header_us"].values())
    print("%-18s%d in %d of %d classes; C++ files with PCH %d of %d" % (
        title + ":", len(found), len(set(n for n, _ in found)), len(base["classes"]),
        sum(1 for g in group_of if g is not None), m["nfiles"]))
    print("  #   class targets   files headers   roots      pch    start commits     cold     warm        G")
    base["groups"] = []
    details = []
    for g, (n, sel) in enumerate(found, 1):
        hs = [h for h in range(len(names)) if sel.selected[comp[h]]]
        roots = selection_roots(base, m, sel)
        base["groups"].append({"class": n, "targets": sorted(sel.members), "headers": [names[h] for h in hs],
                               "roots": [names[h] for h in roots]})
        value, cold, warm = sel.evaluate(sel.parse, sel.build, len(sel.hit), sel.hit_c,
                                         sel.nonhit_p, sel.nonhit_n, sel.ready)
        print("  %-3d %5d %7d %7d %7d %7d %8s %8s %7d %8s %8s %8s%s" % (
            g, n, len(sel.members), sel.n_cons, len(hs), len(roots), duration(sel.build),
            duration(sel.ready + sel.build), len(sel.hit), hours(cold), hours(warm), hours(value),
            "  contrib" if is_contrib(base, sel) else ""))
        dirs = collections.Counter("/".join(t.split(":")[0].split("/")[:4]) for t in sel.members)
        root_parse = []
        for h in roots:
            root_parse.append((sum(p for t, p in zip(base["header_files"][h], base["header_parse"][h])
                                   if sel.cons[t]), h))
        root_parse.sort(reverse=True)
        details.append("  %-3d %s%s; %s" % (
            g, ", ".join("%s %d" % kv for kv in dirs.most_common(_TOP_DIRS)),
            ", ..." if len(dirs) > _TOP_DIRS else "",
            ", ".join("%s %s" % (display(names[h]), hours(us)) for us, h in root_parse[:_TOP_ROOTS])))
    print("%s: targets by dir; roots of the most parse:" % title)
    for line in details:
        print(line)
    print("header parse:     %s; in PCH %s (%.0f%%)" % (hours(total_parse), hours(covered),
                                                       100.0 * covered / max(total_parse, 1)))
    print("  not in PCH:     %s" % ", ".join("%s %s" % (r, hours(us)) for r, us in zip(_REASONS, by_reason)))
    print("top not in PCH (parse, files, reason):")
    for h, us in residual.most_common(_TOP_RESIDUAL):
        print("  %-60s %8s %7d   %s" % (display(names[h]), hours(us), residual_files[h],
                                       residual_reason[h].most_common(1)[0][0]))

    # the includes of the PCH of //build/gn/pch now
    print("current PCH (includes: in each group, in none):")
    by_suffix = collections.defaultdict(list)
    for h, name in enumerate(names):
        parts = name.split("/")
        for i in range(len(parts)):
            by_suffix["/".join(parts[i:])].append(h)
    roots_now = pch_roots()
    for pch in pch_headers():
        spellings = [s for s, ps in roots_now.items() if pch in ps]
        missed = []
        per_group = [0] * len(found)
        for s in sorted(spellings):
            hs = by_suffix.get(s, [])
            hit = [any(sel.selected[comp[h]] for h in hs) for _, sel in found]
            for g, yes in enumerate(hit):
                per_group[g] += yes
            if not any(hit):
                h = max(hs, key=lambda h: residual[h]) if hs else None
                missed.append((residual[h] if h is not None else 0, s,
                               residual_reason[h].most_common(1)[0][0] if h is not None and residual_reason[h] else "-"))
        missed.sort(reverse=True)
        print("  %-16s %d: %s, in none %d%s" % (
            pch, len(spellings), " ".join("%d" % n for n in per_group), len(missed),
            (", e.g. " + "; ".join("%s (%s)" % (display(s), r) for _, s, r in missed[:3])) if missed else ""))


# =============================================================================
# checks
# =============================================================================

_CHECK_ROUNDS = 12
_MACRO_PARTS = 4                # a set of macros changing -E is tried again as these parts
_DIRECTIVE_RE = re.compile(r"^\s*#\s*(define|include|import)\b\s*(.*)$")
_DEFINE_RE = re.compile(r"^([A-Za-z_]\w*)(\([^)]*\))?\s*(.*)$")
_TOKEN_RE = re.compile(r"[A-Za-z_]\w*")
_MARKER_RE = re.compile(r'^# (\d+) "((?:[^"\\]|\\.)*)"((?: \d)*)$')
_INCLUDED_FROM_RE = re.compile(r"^In file included from (.+?):(\d+):$")
_ERROR_RE = re.compile(r"^(.+?):(\d+):\d+: (?:fatal )?error: (.*)$")
_DIAG_RE = re.compile(r"^(.+?):(\d+):\d+: (fatal error|error|note): (.*)$")
_MACRO_LINE_RE = re.compile(r"^#define ([A-Za-z_]\w*)")
_MACRO_DIRECTIVE_RE = re.compile(r"^\s*#\s*(define|undef)\s+([A-Za-z_]\w*)", re.M)
_METADATA_RE = re.compile(r"^!(\d+) = (?:distinct )?!(\w+)\((.*)\)$")
_DBG_RE = re.compile(r"!dbg !(\d+)")
_FIELD_RE = re.compile(r'(\w+): (?:"((?:[^"\\]|\\.)*)"|!(\d+)|(\w+))')
_FOR_O0 = ("-O", "-g", "-gz", "-gsplit-dwarf")
# the link of the empty file with a PCH: what its static initializers need of the libraries
_LINK_FLAGS = ["-shared", "-nodefaultlibs", "-fuse-ld=lld",
               "-Wl,--gc-sections,--no-undefined,--error-limit=0,--no-demangle"]
_SYSTEM_LIBS = ["-lc", "-lm", "-ldl", "-lpthread", "-lrt"]
_UNDEFINED_RE = re.compile(r"error: undefined symbol: (\S+)\s*$")
_REFERENCED_RE = re.compile(r"^>>> referenced by \S+ \((.+):(\d+)\)$")
# words of a demangled name naming no entity of a header
_NOT_NAMES = frozenset("""operator const volatile unsigned signed int long short char bool void float double
    auto typeinfo for name vtable VTT guard variable construction std __1 __cxx11 anonymous namespace non virtual
    thunk to covariant return reference temporary TLS init function wrapper""".split())


def real_path(base, name):
    """The file a normalized name of the traces is."""
    if name.startswith("<out>/"):
        return os.path.join(base["build_dir"], name[len("<out>/"):])
    if name.startswith("<sysroot>/"):
        return os.path.join(base["paths"].sysroot, name[len("<sysroot>/"):])
    return name if os.path.isabs(name) else os.path.join(_ROOT, name)


def spelling(base, name):
    """How the PCH includes a header of the traces."""
    if name.startswith(_LIBCXX):
        return "<%s>" % name[len(_LIBCXX):]
    if name.startswith(_GEN):
        return '"%s"' % name[len(_GEN):]
    if os.path.isabs(name) or name.startswith("<"):
        return '"%s"' % real_path(base, name)
    return '"%s"' % name


def _run(job):
    """Runs one compiler of a check: (kind, key, (returncode, digests of -E
    or {macro: its #define} of -dM or the returncode of the link, stderr,
    includes of -E)). A link is [compile, link]: the link only if the
    compile passed."""
    kind, key, argv, cwd = job
    with tempfile.TemporaryFile("w+", errors="replace") as err:
        if kind == "link":
            rc = subprocess.run(argv[0], cwd=cwd, stdout=subprocess.DEVNULL, stderr=err).returncode
            digests = None if rc else subprocess.run(argv[1], cwd=cwd, stdout=subprocess.DEVNULL,
                                                     stderr=err).returncode
            edges = None
        elif kind == "dM":
            proc = subprocess.Popen(argv, cwd=cwd, stdout=subprocess.PIPE, stderr=err,
                                    universal_newlines=True, errors="replace")
            digests = dict((m.group(1), line) for line in proc.stdout for m in [_MACRO_LINE_RE.match(line)] if m)
            rc = proc.wait()
            edges = None
        elif kind in ("E", "E0"):
            proc = subprocess.Popen(argv, cwd=cwd, stdout=subprocess.PIPE, stderr=err,
                                    universal_newlines=True, errors="replace")
            edges = set() if kind == "E0" else None
            digests = preprocessed_digests(proc.stdout, edges)
            rc = proc.wait()
        else:
            rc = subprocess.run(argv, cwd=cwd, stdout=subprocess.DEVNULL, stderr=err).returncode
            digests = edges = None
        err.seek(0)
        text = err.read()
    return kind, key, (rc, digests, text[-4000:] if kind in ("E", "E0", "dM") else text, edges)


def preprocessed_digests(lines, edges=None):
    """{file as -E names it: digest of what it gives}: its lines but the
    blank ones, the files it includes. Adds to `edges` the pairs (including,
    included)."""
    digests = {}
    stack = []
    current = None
    for line in lines:
        if line.startswith("# "):
            mk = _MARKER_RE.match(line.rstrip("\n"))
            if mk:
                name, flags = mk.group(2), mk.group(3).split()
                if "1" in flags:
                    if current is not None:
                        digests[current].update(b"\0include " + name.encode())
                        if edges is not None:
                            edges.add((current, name))
                    stack.append(current)
                elif "2" in flags and stack:
                    stack.pop()
                current = name
                if name not in digests:
                    digests[name] = hashlib.blake2b(digest_size=16)
                continue
        if current is not None and line.strip():
            digests[current].update(line.encode())
    return dict((k, v.hexdigest()) for k, v in digests.items())


def source_defines(path):
    """[(name, -D argument)] of the #define of a source before its last
    #include."""
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            text = f.read()
    except OSError:
        return []
    found, last = [], -1
    for line in text.replace("\\\n", " ").split("\n"):
        m = _DIRECTIVE_RE.match(line)
        if not m:
            continue
        if m.group(1) != "define":
            last = len(found)
            continue
        d = _DEFINE_RE.match(m.group(2))
        if d:
            body = d.group(3).split("//")[0].strip()
            found.append((d.group(1), "-D%s%s=%s" % (d.group(1), d.group(2) or "", body)))
    return found[:last] if last > 0 else []


def depfile_names(path, paths):
    try:
        with open(path, encoding="utf-8") as f:
            text = f.read().replace("\\\n", " ")
    except OSError:
        return set()
    return set(paths.normalize(p.replace("\\ ", " ")) for p in re.split(r"(?<!\\) ", text.partition(":")[2]) if p)


def static_inits(path, paths):
    """[(header, line)] of the dynamic initializers and constructors of a
    module of -emit-llvm with line tables."""
    meta, ctors, bodies = {}, [], {}
    current = None
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            if line.startswith("!"):
                mt = _METADATA_RE.match(line.rstrip("\n"))
                if mt:
                    meta[int(mt.group(1))] = (mt.group(2), mt.group(3))
            elif line.startswith("@llvm.global_ctors"):
                ctors = re.findall(r"ptr @([\w.$]+)", line)
            elif line.startswith("define "):
                name = re.search(r'@([\w.$]+|"[^"]+")\(', line).group(1).strip('"')
                current = None
                if name.startswith("__cxx_global_var_init") or name in ctors:
                    current = name
                    sp = _DBG_RE.search(line.rpartition(")")[2])
                    bodies[name] = [int(sp.group(1)) if sp else None, []]
            elif line.startswith("}"):
                current = None
            elif current and len(bodies[current][1]) < 16:
                d = _DBG_RE.search(line)
                if d:
                    bodies[current][1].append(int(d.group(1)))

    def fields(n):
        return dict((k, int(r) if r else w if w else s)
                    for k, s, r, w in _FIELD_RE.findall(meta.get(n, ("", ""))[1]))

    def file_of(n):
        f = fields(fields(n).get("file"))
        return paths.normalize(os.path.join(f.get("directory", ""), f.get("filename", "")))

    out = []
    for name, (sp, locs) in bodies.items():
        if name.startswith("_GLOBAL__sub_I_") or not (name in ctors or name.startswith("__cxx_global_var_init")):
            continue
        # line 0: code of no line, e.g. the guard of a template's static member
        loc = next((d for d in locs if fields(d).get("line") not in (None, "0")), None)
        if loc is not None:
            while "inlinedAt" in fields(loc):
                loc = fields(loc)["inlinedAt"]
            scope, line = fields(loc).get("scope"), fields(loc).get("line")
            out.append((file_of(scope), line))
        elif sp is not None:
            out.append((file_of(sp), fields(sp).get("line")))
    return out


def file_tokens(base, name):
    """The names of a header of the traces."""
    tokens = base.setdefault("tokens", {})
    if name not in tokens:
        try:
            with open(real_path(base, name), encoding="utf-8", errors="replace") as f:
                tokens[name] = frozenset(_TOKEN_RE.findall(f.read()))
        except OSError:
            tokens[name] = frozenset()
    return tokens[name]


def group_inputs(base, sel):
    """(files, roots of the PCH in the order it includes them, the -D and -U
    all the files have, the include dirs of the files) of a group."""
    m = base["model"]
    names, objs = base["header_names"], base["objs"]
    files = [t for t in range(m["nfiles"]) if sel.cons[t]]

    def order(h):
        name = names[h]
        return (0 if name.startswith(_LIBCXX) or name.startswith("<sysroot>") or os.path.isabs(name) else
                1 if name.startswith("contrib/") else 2 if name.startswith(("util/", "library/")) else
                4 if name.startswith("<out>") else 3, name)
    roots = sorted(selection_roots(base, m, sel), key=order)

    defines = [base["defines_of"][objs[t]] for t in files]
    common = set(defines[0]) if defines else set()
    for d in defines[1:]:
        common.intersection_update(d)
    defines = [d for d in defines[0] if d in common] if defines else []
    lists = collections.Counter(tuple(base["include_dirs_of"][objs[t]]) for t in files)
    include_dirs = []
    for dirs, _ in lists.most_common():
        for d in dirs:
            if d not in include_dirs:
                include_dirs.append(d)
    return files, roots, defines, include_dirs


class Probe(object):
    """What the checks compile for a group: a source including the roots of
    its PCH, with the flags of its class, the -D all its files have and the
    include dirs of its files."""

    def __init__(self, base, n, sel, tmp, key):
        m = base["model"]
        names, comp = base["header_names"], m["comp"]
        self.headers = [h for h in range(len(names)) if sel.selected[comp[h]]]
        self.header_set = frozenset(self.headers)
        self.files, self.roots, self.defines, self.include_dirs = group_inputs(base, sel)

        self.source = os.path.realpath(os.path.join(tmp, "probe%d.cpp" % key))
        self.header = os.path.realpath(os.path.join(tmp, "probe%d.h" % key))
        for path in (self.source, self.header):
            with open(path, "w") as f:
                for h in self.roots:
                    f.write("#include %s\n" % spelling(base, names[h]))
        flags = list(base["class_flags"][n - 1])
        includes = [a for opt, d in self.include_dirs for a in (opt, d)]
        self.common = [base["compiler"]] + flags + self.defines + includes + ["-w"]
        self.ll = os.path.join(tmp, "probe%d.ll" % key)
        self.depfile = self.ll + ".d"
        o0 = [a for a in flags if not a.startswith(_FOR_O0)]
        self.compile = ([base["compiler"]] + o0 + self.defines + includes +
                        ["-w", "-ferror-limit=0", "-O0", "-gline-tables-only", "-S", "-emit-llvm",
                         "-MD", "-MF", self.depfile, self.source, "-o", self.ll])
        # as //build/gn/pch builds it
        self.pch = os.path.join(tmp, "probe%d.pch" % key)
        self.build_pch = self.common + ["-ferror-limit=0", "-x", "c++-header", "-fpch-instantiate-templates",
                                        "-Xclang", "-fno-pch-timestamp", self.header, "-o", self.pch]
        self.macros = self.common + ["-dM", "-E", self.header]
        empty = os.path.join(tmp, "empty.cpp")
        open(empty, "w").close()
        obj, so = os.path.join(tmp, "link%d.o" % key), os.path.join(tmp, "link%d.so" % key)
        self.link = [self.common + ["-include-pch", self.pch, "-c", empty, "-o", obj],
                     [base["compiler"]] + [a for a in flags if a.startswith("--sysroot")] + _LINK_FLAGS +
                     [obj, "-o", so] + _SYSTEM_LIBS]
        # a file passing with a PCH of these -D and include dirs passes with fewer headers
        self.key = (tuple(self.defines), tuple(self.include_dirs))
        self.flags, self.base = flags, base

    def preprocess(self, macros=()):
        return self.common + list(macros) + ["-E", self.source]

    def with_pch(self, t):
        """Checks a file of the build with the PCH: its own command."""
        return self.base["argv_of"][self.base["objs"][t]] + ["-include-pch", self.pch, "-fsyntax-only",
                                                             "-ferror-limit=20"]

    def alone(self, t, what):
        """The file without the PCH: -E for the files it parses, -dM for its macros."""
        return self.base["argv_of"][self.base["objs"][t]] + (["-E"] if what == "E" else ["-dM", "-E"])


class _MacroSet(object):
    """Macros -E of a probe runs with together: the files they change, its
    error, what its parts explain."""

    def __init__(self, g, macros, parent):
        self.g, self.macros, self.parent = g, macros, parent
        self.pending = 0
        self.diff, self.error = set(), None
        self.explained, self.error_explained = set(), False


def check_round(args, base, groups, tmp):
    """Compiles the probes of `groups` [(class, selection)]: their problems as
    {group: {"errors": {header: message}, "inits": {header: where},
    "macros": {name: headers depending on it}, "targets": {name: targets
    having it}, "conflicts": {target: [(header or None, message)]} -- the
    files of the target failing with the PCH, "files": counts of the files,
    "missing", "extra", "failed", "edges": includes -E shows}} -- headers as
    ids.

    The macros: -E with all the macros of the group the files do not all
    have; a set changing some file or failing is tried as parts, down to one
    macro; what no part explains is put on all the macros of the set."""
    m = base["model"]
    names, objs = base["header_names"], base["objs"]
    header_id = base["header_id"]
    normalize = base["paths"].normalize
    defines_in = base.setdefault("source_defines", {})

    def tokens_of(name):
        return file_tokens(base, name)

    probes, macros_of = {}, {}
    for g, (n, sel) in groups.items():
        p = probes[g] = Probe(base, n, sel, tmp, g)
        known = set()
        for h in p.headers:
            known |= tokens_of(names[h])
        common_names = set(d[2:].partition("=")[0] for d in p.defines)
        values = collections.defaultdict(collections.Counter)    # name -> -D argument -> files
        having = collections.defaultdict(set)                    # name -> targets
        for t in p.files:
            obj = objs[t]
            for d in base["defines_of"][obj]:
                name = d[2:].partition("=")[0]
                if d.startswith("-D") and d not in p.defines and name in known:
                    values[name][d] += 1
                    having[name].add(base["target_of"][obj])
            src = base["source_of"][obj]
            if src not in defines_in:
                defines_in[src] = source_defines(os.path.join(base["build_dir"], src))
            for name, arg in defines_in[src]:
                if name in known and name not in common_names:
                    values[name][arg] += 1
                    having[name].add(base["target_of"][obj])
        macros_of[g] = (dict((name, c.most_common(1)[0][0]) for name, c in values.items()), having)

    # each group goes on as soon as its own runs are done
    results, waiting = {}, collections.defaultdict(list)
    found = collections.defaultdict(lambda: collections.defaultdict(lambda: [set(), []]))  # g -> macro -> [files, errors]
    running, sets = set(), []                   # sets: by the number the runs of -E carry
    with_pch = set()
    runs = [0]
    passed = base.setdefault("passed_with", {})     # file -> (probe key, headers) it passed with
    skipped = collections.Counter()                 # group -> files passed before
    executor = concurrent.futures.ProcessPoolExecutor(args.check_jobs)

    def submit(kind, key, argv):
        runs[0] += 1
        running.add(executor.submit(_run, (kind, key, argv, base["build_dir"])))

    def try_set(g, macros, parent):
        s = _MacroSet(g, tuple(macros), parent)
        sets.append(s)
        submit("E", len(sets) - 1, probes[g].preprocess([macros_of[g][0][c] for c in s.macros]))

    def finish(s):
        if len(s.macros) == 1 or not s.pending:
            for c in s.macros:
                found[s.g][c][0].update(s.diff - s.explained)
                if s.error and not s.error_explained:
                    found[s.g][c][1].append(s.error)
        q = s.parent
        if q is not None:
            q.explained |= s.diff
            q.error_explained |= s.error is not None
            q.pending -= 1
            if not q.pending:
                finish(q)

    def done(s, res):
        rc, digests, err, _ = res
        d0 = results[("E0", s.g)][1]
        if rc:
            s.error = next((me for me in map(_ERROR_RE.match, err.splitlines()) if me), None)
            s.error = (normalize(s.error.group(1)), s.error.group(0)) if s.error else (None, (
                err.strip().splitlines() or ["failed"])[-1])
        else:
            s.diff = set(f for f in set(d0) | set(digests) if d0.get(f) != digests.get(f))
        if (s.diff or s.error) and len(s.macros) > 1:
            k = min(_MACRO_PARTS, len(s.macros))
            parts = [s.macros[i * len(s.macros) // k:(i + 1) * len(s.macros) // k] for i in range(k)]
            s.pending = len(parts)
            for part in parts:
                try_set(s.g, part, s)
        else:
            finish(s)

    def advance(g):
        c, e0, pc = results.get(("compile", g)), results.get(("E0", g)), results.get(("pch", g))
        if c is not None and pc is not None and not c[0] and not pc[0] and g not in with_pch:
            with_pch.add(g)
            p = probes[g]
            submit("link", g, p.link)
            for t in p.files:
                key, hs = passed.get(t, (None, ()))
                if key == p.key and p.header_set <= hs:
                    skipped[g] += 1
                else:
                    submit("tu", (g, t), p.with_pch(t))
        if c is None or e0 is None:
            return
        if c[0] or e0[0]:
            waiting[g] = []                     # the rest after the errors are gone
            return
        while waiting[g]:
            done(*waiting[g].pop())

    for g, p in sorted(probes.items()):
        submit("compile", g, p.compile)
        submit("pch", g, p.build_pch)
        submit("E0", g, p.preprocess())
        if macros_of[g][0]:
            try_set(g, sorted(macros_of[g][0]), None)
    try:
        while running:
            finished, _ = concurrent.futures.wait(running, return_when=concurrent.futures.FIRST_COMPLETED)
            for fut in finished:
                running.discard(fut)
                kind, key, res = fut.result()
                if kind == "E":
                    waiting[sets[key].g].append((sets[key], res))
                    advance(sets[key].g)
                elif kind in ("tu", "link"):
                    results[(kind, key)] = res
                else:
                    results[(kind, key)] = res
                    advance(key)
    finally:
        for fut in running:
            fut.cancel()
        executor.shutdown()
    log("checks: %d compiler runs" % runs[0])

    out, by_macros = {}, []                     # by_macros: (group, file, target, message, where)
    links = dict((g, results[("link", g)]) for g in groups if ("link", g) in results)
    for g, (n, sel) in groups.items():
        p = probes[g]
        macros, having = macros_of[g]
        rc_e, d0, err_e, edges = results[("E0", g)]
        r = out[g] = {"errors": {}, "inits": {}, "macros": collections.defaultdict(set), "conflicts": {},
                      "targets": having, "missing": 0, "extra": 0, "failed": None, "edges": edges or (),
                      "files": collections.Counter()}
        rc, _, err, _ = results[("compile", g)]
        rc_pch, _, err_pch, _ = results[("pch", g)]
        for code, text in ((rc, err), (rc_pch, err_pch)):
            chain = None
            for line in text.splitlines():
                mi = _INCLUDED_FROM_RE.match(line)
                if mi and chain is None:
                    chain = mi
                    continue
                me = _ERROR_RE.match(line)
                if me:
                    where = chain or me
                    msg = "%s:%s: %s" % (display(normalize(me.group(1))), me.group(2), me.group(3))
                    h = header_id.get(normalize(me.group(1)))
                    if os.path.realpath(where.group(1)) in (p.source, p.header):
                        r["errors"].setdefault(p.roots[int(where.group(2)) - 1], msg)
                    elif h is not None:
                        r["errors"].setdefault(h, msg)
                    elif r["failed"] is None:
                        r["failed"] = line
                if not line.startswith(("In file included from", "    ")):
                    chain = None
            if code and not r["errors"] and r["failed"] is None:
                r["failed"] = (text.strip().splitlines() or ["the compiler failed"])[-1]
        if rc or rc_pch:
            continue
        # a file failing with the PCH: put on the first header of the PCH its
        # first error and the notes of it name; else on the macros (below)
        in_pch = set(p.headers)
        fc = r["files"]
        fc["total"] += len(p.files)
        fc["passed before"] += skipped[g]
        for t in p.files:
            if ("tu", (g, t)) not in results:
                continue
            fc["checked"] += 1
            rc_t, _, err_t, _ = results[("tu", (g, t))]
            if not rc_t:
                passed[t] = (p.key, p.header_set)
                continue
            fc["failing"] += 1
            target = base["target_of"][objs[t]]
            h = msg = None
            where = []                          # (file, line) of the error and its notes
            for line in err_t.splitlines():
                md = _DIAG_RE.match(line)
                if not md:
                    continue
                if msg is not None and md.group(3) != "note":
                    break
                if msg is None:
                    msg = "%s:%s: %s" % (display(normalize(md.group(1))), md.group(2), md.group(4))
                where.append((normalize(md.group(1)), int(md.group(2))))
                hh = header_id.get(normalize(md.group(1)))
                if h is None and hh in in_pch:
                    h = hh
            msg = "%s: %s" % (display(normalize(base["source_of"][objs[t]])),
                              msg or (err_t.strip().splitlines() or ["the compiler failed"])[-1])
            if h is None and any(w in msg for w in ("precompiled", "PCH", "AST file")) and r["failed"] is None:
                r["failed"] = "%s with the PCH: %s" % (target, msg)
            if h is not None:
                fc["on headers"] += 1
                r["conflicts"].setdefault(target, []).append((h, msg))
            else:
                by_macros.append((g, t, target, msg, where))
        reached = depfile_names(p.depfile, base["paths"])
        in_pch = set(names[h] for h in p.headers)
        r["missing"] = len(in_pch - reached)
        r["extra"] = len(reached - in_pch - set([normalize(p.source)]))
        for name, line in static_inits(p.ll, base["paths"]):
            h = header_id.get(name)
            if h is None:
                if r["failed"] is None:
                    r["failed"] = "a static initializer in %s:%s, no header of the traces" % (name, line)
                continue
            if sum(1 for t in base["header_files"][h] if sel.cons[t]) < sel.n_cons:
                r["inits"].setdefault(h, "%s:%s" % (display(name), line))
        if rc_e:
            if r["failed"] is None:
                r["failed"] = "-E: %s" % (err_e.strip().splitlines() or ["failed"])[-1]
            continue
        for c, (files, errors) in found[g].items():
            for f in files:
                h = header_id.get(normalize(f))
                if h is not None:                   # else a header no traced file parses: its includer differs too
                    r["macros"][c].add(h)
            for name, line in errors:
                h = header_id.get(name) if name else None
                if h is not None:
                    r["macros"][c].add(h)
                elif r["failed"] is None:
                    r["failed"] = "-E with %s: %s" % (macros[c], line)
    put_on_macros(args, base, probes, by_macros, out)
    link_conflicts(args, base, probes, links, out)
    return out


def put_on_macros(args, base, probes, failing, out):
    """The files failing with the PCH on no header of it: on the headers of
    the PCH with a #define or #undef of a macro the PCH has otherwise than the
    file alone, of the names on the lines of the error and its notes."""
    names = base["header_names"]
    directives = base.setdefault("macro_directives", {})   # header -> (#define names, #undef names)
    lines_of = {}

    def directives_of(h):
        if names[h] not in directives:
            try:
                with open(real_path(base, names[h]), encoding="utf-8", errors="replace") as f:
                    found = _MACRO_DIRECTIVE_RE.findall(f.read())
            except OSError:
                found = []
            directives[names[h]] = (frozenset(n for k, n in found if k == "define"),
                                    frozenset(n for k, n in found if k == "undef"))
        return directives[names[h]]

    def tokens_at(name, line):
        if name not in lines_of:
            try:
                with open(real_path(base, name), encoding="utf-8", errors="replace") as f:
                    lines_of[name] = f.read().split("\n")
            except OSError:
                lines_of[name] = []
        text = lines_of[name]
        return set(_TOKEN_RE.findall(text[line - 1])) if 0 < line <= len(text) else set()

    if not failing:
        return
    results = {}
    with concurrent.futures.ProcessPoolExecutor(args.check_jobs) as executor:
        jobs = [("dM", ("pch", g), probes[g].macros) for g in sorted(set(f[0] for f in failing))]
        jobs += [("dM", ("file", t), probes[g].alone(t, "dM")) for g, t, _, _, _ in failing]
        for kind, key, res in executor.map(_run, [(k, key, argv, base["build_dir"]) for k, key, argv in jobs]):
            results[key] = res
    log("checks: %d compiler runs for the macros" % len(jobs))
    for g, t, target, msg, where in failing:
        r = out[g]
        pch, alone = results[("pch", g)][1] or {}, results[("file", t)][1] or {}
        near = set()
        for name, line in where:
            near |= tokens_at(name, line)
        defined = set(c for c in near if c in pch and alone.get(c) != pch[c])
        removed = set(c for c in near if c in alone and c not in pch)
        hs = set()
        if defined or removed:
            for h in probes[g].headers:
                defs, undefs = directives_of(h)
                if defined & defs or removed & undefs:
                    hs.add(h)
        r["files"]["via macros" if hs else "unattributed"] += 1
        for h in sorted(hs) or [None]:
            r["conflicts"].setdefault(target, []).append((h, msg))


def link_lines(base):
    """({output of the manifest: its target}, {target: [the targets on the
    line of each link taking its objects]}) of the baseline."""
    if "link_lines" not in base:
        producer = target_graph(base)[0]
        lines = collections.defaultdict(list)
        for e in base["edges"]:
            if e.rule not in ("solink", "link"):
                continue
            own = producer.get(e.outputs[0])
            line = frozenset(t for t in [own] + [producer.get(p) for p in e.inputs] if t)
            for t in set(producer.get(p) for p in e.inputs if p.endswith(".o")) | set([own]):
                if t:
                    lines[t].append(line)
        base["link_lines"] = (producer, lines)
    return base["link_lines"]


def definers(args, base, symbols):
    """{symbol: the targets of the baseline whose library defines it}: none
    for one of the system libraries."""
    known = base.setdefault("definers", {})
    new = set(symbols) - set(known)
    if not new:
        return known
    producer = link_lines(base)[0]
    libs = [(e.outputs[0], e.rule) for e in base["edges"] if e.rule in ("solink", "alink")]

    def scan(lib):
        path, rule = lib
        argv = ["nm", "--defined-only", "-P"] + (["-D"] if rule == "solink" else []) + [
            os.path.join(base["build_dir"], path)]
        text = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, universal_newlines=True,
                              errors="replace").stdout
        return producer.get(path), new.intersection(line.split(" ", 1)[0] for line in text.splitlines()
                                                    if line and not line.endswith(":"))
    for s in new:
        known[s] = set()
    with concurrent.futures.ThreadPoolExecutor(min(8, args.check_jobs)) as executor:
        for t, found in executor.map(scan, libs):
            for s in found if t else ():
                known[s].add(t)
    log("checks: %d libraries scanned for %d symbols" % (len(libs), len(new)))
    return known


def demangled(symbols):
    """{symbol: as c++filt reads it}."""
    symbols = sorted(symbols)
    text = subprocess.run(["c++filt"], input="\n".join(symbols) + "\n", stdout=subprocess.PIPE,
                          universal_newlines=True, errors="replace").stdout.split("\n")
    return dict((s, text[i] if i < len(text) and text[i] else s) for i, s in enumerate(symbols))


def link_conflicts(args, base, probes, links, out):
    """The targets of a group having a link that lacks a library the PCH
    needs: the empty file with the PCH, linked alone, needs the symbols of
    its static initializers. Put on the headers of the PCH having the rarest
    name of the symbol among them, of the dir of a library defining it
    first; else on the header referencing it."""
    names, objs, header_id = base["header_names"], base["objs"], base["header_id"]
    normalize = base["paths"].normalize
    needs = {}                                  # group -> {symbol: header referencing it}
    for g, (rc, link_rc, text, _) in sorted(links.items()):
        r = out[g]
        if rc:
            r["failed"] = r["failed"] or "the empty file with the PCH: %s" % (
                text.strip().splitlines() or ["failed"])[-1]
            continue
        found, symbol = {}, None
        for line in text.splitlines():
            mu = _UNDEFINED_RE.search(line)
            if mu:
                symbol = mu.group(1)
                found.setdefault(symbol, None)
                continue
            mr = _REFERENCED_RE.match(line)
            if mr and symbol is not None and found[symbol] is None:
                found[symbol] = header_id.get(normalize(mr.group(1)))
        if link_rc and not found:
            r["failed"] = r["failed"] or "the link of the empty file with the PCH: %s" % (
                text.strip().splitlines() or ["failed"])[-1]
            continue
        needs[g] = found
    defined = definers(args, base, set().union(*needs.values())) if needs else {}
    readable = demangled(s for found in needs.values() for s in found if defined[s])
    lines = link_lines(base)[1]
    for g, found in sorted(needs.items()):
        p, r = probes[g], out[g]
        need = dict((s, frozenset(defined[s])) for s in found if defined[s])
        culprits = {}                           # symbol -> headers

        def culprits_of(s):
            if s not in culprits:
                words = set(_TOKEN_RE.findall(readable[s])) - _NOT_NAMES
                dirs = tuple(t[2:].partition(":")[0] + "/" for t in need[s])
                own = [h for h in p.headers if names[h].startswith(dirs) or
                       names[h].startswith(_GEN) and names[h][len(_GEN):].startswith(dirs)]
                best = None
                for pool in (own, p.headers):
                    for w in words:
                        hs = [h for h in pool if w in file_tokens(base, names[h])]
                        if hs and (best is None or len(hs) < len(best)):
                            best = hs
                    if best:
                        break
                culprits[s] = best or [found[s] if found[s] in p.header_set else None]
            return culprits[s]
        members = set(base["target_of"][objs[t]] for t in p.files)
        for target in sorted(members):
            if not lines.get(target):
                continue
            r["files"]["links"] += 1
            lacking = sorted(s for s in need for line in lines[target] if not need[s] & line)
            if not lacking:
                continue
            r["files"]["links lacking"] += 1
            for s in sorted(set(lacking)):
                msg = "%s: a link has no %s for %s" % (target, " or ".join(sorted(need[s])), readable[s][:120])
                for h in culprits_of(s):
                    r["conflicts"].setdefault(target, []).append((h, msg))


def checks(args, base):
    names = base["header_names"]
    header_id = base["header_id"] = dict((name, h) for h, name in enumerate(names))
    normalized = {}

    def normalize(name):
        if name not in normalized:
            normalized[name] = base["paths"].normalize(name)
        return normalized[name]
    def bans(m, headers):
        return banned_units(m, set(m["comp"][h] for h in headers))

    def choose(m, sel, members, now, hs, have):
        """True to drop the headers `hs` (with the ones including them), False
        to move the targets `have` out: what gives more G, the headers of `sel`
        only losing some."""
        # TODO: new include edges rebuild only the groups they touch; time per
        # phase in round lines.
        headers = frozenset(h for h in range(len(names)) if sel.selected[m["comp"][h]])
        a = shrunk(m, members, headers, bans(m, now | hs)).value
        b = shrunk(m, members - have, headers, bans(m, now)).value if members - have else 0
        return a >= b

    groups = dict(enumerate(base["found"]))     # id -> (class, selection)
    # what the checks found stays with the targets: a group made again of
    # them has no PCH with the headers dropped for them or breaking them; the
    # targets moved for a -D go together
    target_bans = collections.defaultdict(set)  # target -> headers
    moved_for = collections.defaultdict(set)    # target -> -D
    moved = set()                               # targets moved out of a group
    contrib, contrib_bans = base["contrib"], base["contrib_bans"]
    for target in contrib:
        target_bans[target] |= contrib_bans

    def group_bans(members):
        return set().union(*(target_bans[t] for t in members)) if members else set()

    def signature(sel):
        comp = base["model"]["comp"]
        return frozenset(sel.members), frozenset(h for h in range(len(names)) if sel.selected[comp[h]])
    checked = {}                                # group -> signature of what was checked
    no_pch = set()                              # targets failing with any PCH
    inits = {}                                  # header -> where: kept, link_conflicts() checks what they need
    next_id = [len(groups)]
    pending = set(groups)
    examples = collections.OrderedDict()
    totals = collections.Counter()
    rounds = 0
    tmp = tempfile.mkdtemp(prefix="pch_update.")
    try:
        while pending and rounds < _CHECK_ROUNDS:
            rounds += 1
            m = base["model"]
            log("checks, round %d: %d groups..." % (rounds, len(pending)))
            found = check_round(args, base, dict((g, groups[g]) for g in pending), tmp)
            for g in found:
                checked[g] = signature(groups[g][1])
            counts, files = collections.Counter(), collections.Counter()
            leftovers = collections.defaultdict(set)    # class -> targets
            changed = set()
            # includes the traces miss: a header included again after its guard
            learned = 0
            for r in found.values():
                for a, b in r["edges"]:
                    ha, hb = header_id.get(normalize(a)), header_id.get(normalize(b))
                    if ha is not None and hb is not None and ha != hb and (ha << 32 | hb) not in base["header_edges"]:
                        base["header_edges"].add(ha << 32 | hb)
                        learned += 1
            counts["includes learned"] = learned
            for g, r in sorted(found.items()):
                n, sel = groups[g]
                counts["not reached"] += r["missing"]
                counts["reached, not in PCH"] += r["extra"]
                files.update(r["files"])
                if r["failed"]:
                    counts["failed"] += 1
                    examples.setdefault("failed", r["failed"])
                    leftovers[n] |= sel.members
                    del groups[g]
                    continue
                ban = set()
                inits.update(r["inits"])
                counts["static init (kept)"] += len(r["inits"])
                for where in list(r["inits"].values())[:1]:
                    examples.setdefault("static init (kept)", where)
                # the PCH fails to compile with the flags of the group
                for h, msg in r["errors"].items():
                    counts["compile errors"] += 1
                    examples.setdefault("compile errors", msg)
                    ban.add(h)
                members = set(sel.members)
                # a file of a target fails with the PCH: drop the header or move the targets it breaks
                by_header = collections.defaultdict(set)
                for target, failures in sorted(r["conflicts"].items()):
                    counts["targets failing"] += 1
                    examples.setdefault("targets failing", "%s: %s" % (target, failures[0][1]))
                    if any(h is None for h, _ in failures):
                        no_pch.add(target)
                        members.discard(target)
                        examples.setdefault("unattributed", next(msg for h, msg in failures if h is None))
                        continue
                    for h, _ in failures:
                        by_header[h].add(target)
                for h, have in sorted(by_header.items(), key=lambda kv: -len(kv[1])):
                    have &= members
                    if not have or h in ban:
                        continue
                    if choose(m, sel, members, group_bans(members) | ban, set([h]), have):
                        ban.add(h)
                        counts["failing: headers dropped"] += 1
                    else:
                        members -= have
                        moved |= have
                        for target in have:
                            target_bans[target].add(h)
                        counts["failing: targets moved"] += len(have)
                # a header depending on a -D some files have: drop it or move the targets having it
                for name, hs in sorted(r["macros"].items(), key=lambda kv: -len(kv[1])):
                    counts["-D"] += 1
                    have = r["targets"][name] & members
                    hs = hs - ban
                    if not have or not hs:
                        continue
                    if choose(m, sel, members, group_bans(members) | ban, hs, have):
                        ban |= hs
                        counts["-D: headers dropped"] += len(hs)
                    else:
                        members -= have
                        moved |= have
                        for target in have:
                            moved_for[target].add(name)
                        counts["-D: targets moved"] += len(have)
                    examples.setdefault("-D", "%s in %s, targets with it %d" % (
                        name, display(names[min(hs)]), len(have)))
                if not ban and members == sel.members:
                    continue
                for target in members:
                    target_bans[target] |= ban
                groups[g] = (n, sel, members)
                changed.add(g)
            # a group only loses headers: the files passed stay passed
            had = dict((g, frozenset(h for h in range(len(names)) if groups[g][1].selected[m["comp"][h]]))
                       for g in groups)
            if learned:
                # new units: every group takes its headers again
                base["model"] = m = build_model(base)
                changed = set(groups)
            for g in sorted(changed):
                n, sel = groups[g][:2]
                members = groups[g][2] if len(groups[g]) > 2 else sel.members
                new = shrunk(m, members, had[g], bans(m, group_bans(members)))
                kept = new.members if new.value > 0 else set()
                leftovers[n] |= sel.members - kept
                if kept:
                    groups[g] = (n, new)
                else:
                    del groups[g]
            # the same targets and headers as checked: nothing to check again
            changed = set(g for g in changed if g in groups and checked.get(g) != signature(groups[g][1]))
            for n, targets in sorted(leftovers.items()):
                parts = collections.defaultdict(list)
                for target in sorted(targets - no_pch):
                    parts[(frozenset(target_bans[target]), frozenset(moved_for[target]))].append(target)
                for (hs, _), part in sorted(parts.items(), key=lambda kv: kv[1][0]):
                    if set(part) <= contrib:
                        sels = [sel for sel in [form_group(m, part, bans(m, hs))] if sel.value > 0]
                    else:
                        sels = groups_of_class(m, part, bans(m, hs))
                    for sel in sels:
                        g = next_id[0]
                        next_id[0] += 1
                        groups[g] = (n, sel)
                        changed.add(g)
            totals.update(counts)
            files_text = "files checked %d of %d (passed before %d), failing %d: on headers %d, via macros %d, " \
                         "unattributed %d; targets whose link lacks a library %d of %d" % (
                             files["checked"], files["total"], files["passed before"], files["failing"],
                             files["on headers"], files["via macros"], files["unattributed"],
                             files["links lacking"], files["links"])
            print("%-18s%s" % ("checks:" if rounds == 1 else "", "round %d: %d groups; %s; %s" % (
                rounds, len(found), files_text, ", ".join("%s %d" % (k, counts[k]) for k in (
                    "compile errors", "targets failing", "failing: headers dropped", "failing: targets moved",
                    "-D", "-D: headers dropped", "-D: targets moved", "static init (kept)", "failed",
                    "not reached", "reached, not in PCH", "includes learned")))))
            pending = changed
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    for k, v in examples.items():
        print("  e.g. %s: %s" % (k, v))
    for g in pending:
        del groups[g]
    base["found"] = [groups[g] for g in sorted(groups)]
    print("checked:          %d groups pass after %d rounds, unresolved %d; headers dropped %d, "
          "targets moved %d, with no PCH %d, with static init %d" % (
              len(groups), rounds, len(pending), len(set().union(*target_bans.values()) - contrib_bans) if target_bans else 0,
              len(moved), len(no_pch), len(inits)))
    report_groups(base, "checked groups")
    return not pending

# =============================================================================
# writing
# =============================================================================

_MIN_GROUP_G = 60 * 10**6       # us of G: a group saving less has no PCH
_GROUPS_GNI = os.path.join(_PCH_DIR, "groups.gni")
_CONTRIB = "contrib"            # the name of the group of the targets without //util
_TARGETS_GNI = os.path.join(_PCH_DIR, "targets.gni")
_WRITTEN = "written by build/gn/scripts/pch_update.py"
_GNI_NAME_RE = re.compile(r'^\s*name = "([^"]*)"\s*$')
_GNI_LIST_RE = re.compile(r"^\s*(\w+) = \[(.*?)(\]?)\s*$")
_GNI_STRING_RE = re.compile(r'"([^"]*)"')
_PCH_DEPS = ["//contrib/libs/linux-headers:linux-headers", "//contrib/libs/cxxsupp/libcxx:libcxx"]


def label_matches(label, patterns):
    """label_matches() of gn for a label without a toolchain."""
    d, _, name = label.partition(":")
    for p in patterns:
        pd, _, pn = p.partition(":")
        if pd.endswith("/*") and not pn:
            if d == pd[:-2] or d.startswith(pd[:-1]):
                return True
        elif pd == d and pn in ("*", name):
            return True
    return False


def gni_lists(path, key):
    """The string lists `key` of a .gni: [(name of the scope or None, [strings])]."""
    out, name, current = [], None, None
    if not os.path.exists(path):
        return out
    with open(path, encoding="utf-8") as f:
        for line in f:
            m = _GNI_NAME_RE.match(line)
            if m:
                name = m.group(1)
                continue
            m = _GNI_LIST_RE.match(line)
            if m and current is None:
                if m.group(1) == key:
                    items = _GNI_STRING_RE.findall(m.group(2))
                    if m.group(3):
                        out.append((name, items))
                    else:
                        current = items
                continue
            if current is not None:
                if line.strip().startswith("]"):
                    out.append((name, current))
                    current = None
                else:
                    current.extend(_GNI_STRING_RE.findall(line))
    return out


def gn_escape(text):
    """A string as gn reads it back as is."""
    return text.replace("\\", "\\\\").replace('"', '\\"').replace("$", "\\$")


def gn_list(key, items, indent):
    """A list as gn format writes it."""
    pad = " " * indent
    if not items:
        return "%s%s = []\n" % (pad, key)
    if len(items) == 1:
        return '%s%s = [ "%s" ]\n' % (pad, key, items[0])
    return "%s%s = [\n%s%s]\n" % (pad, key, "".join('%s  "%s",\n' % (pad, i) for i in items), pad)


def fold(targets, all_targets):
    """Patterns of gn matching `targets` and none of the rest of `all_targets`:
    a dir as //dir/* when all the targets of 2 or more under it are there."""
    under = collections.defaultdict(lambda: [0, 0])     # dir -> [targets under it, of them in `targets`]
    for t in all_targets:
        parts = t.partition(":")[0][2:].split("/")
        for i in range(1, len(parts) + 1):
            c = under["/".join(parts[:i])]
            c[0] += 1
            c[1] += t in targets
    out = set()
    for t in targets:
        parts = t.partition(":")[0][2:].split("/")
        full = next(("/".join(parts[:i]) for i in range(1, len(parts) + 1)
                     if under["/".join(parts[:i])][0] >= 2 and under["/".join(parts[:i])][0] == under["/".join(parts[:i])][1]),
                    None)
        out.add("//%s/*" % full if full else t)
    return sorted(out)


def target_graph(base):
    """({output of the manifest: its target}, {target: the targets it depends
    on}, the executables): from the build dir with pch_mode = "none", where
    the tools are built in the default toolchain."""
    # gn writes the edges of small targets to toolchain.ninja: their target
    # is the one of the phony phony/<dir>/<name> having their outputs
    # (the one listing files before one of a group listing phony targets)
    phonies = [e for e in base["edges"] if e.rule == "phony" and e.outputs[0].startswith("phony/")]
    label = dict((e.outputs[0], "//%s:%s" % e.outputs[0][len("phony/"):].rpartition("/")[::2]) for e in phonies)
    listed = {}
    for e in sorted(phonies, key=lambda e: any(p in label for p in e.inputs)):
        for p in e.inputs:
            listed.setdefault(p, label[e.outputs[0]])

    def target(e):
        if e.rule == "phony":
            return label.get(e.outputs[0])
        return listed.get(e.outputs[0], e.target) if e.target == "toolchain.ninja" else e.target

    producer = dict(label)
    for e in base["edges"]:
        if e.rule != "phony":
            for o in e.outputs:
                producer[o] = target(e)
    deps, exe = collections.defaultdict(set), set()
    for e in base["edges"]:
        a = target(e)
        if a is None:
            continue
        if e.rule == "link":
            exe.add(a)
        for p in e.inputs + e.order_only:
            b = producer.get(p)
            if b is not None and b != a:
                deps[a].add(b)
    return producer, deps, exe


def reachable(deps, exe, start):
    """The targets `start` depend on, not through an executable: with
    pch_mode = "global" the tools are of tool_toolchain."""
    seen, stack = set(start), list(start)
    while stack:
        for b in deps.get(stack.pop(), ()):
            if b not in seen and b not in exe:
                seen.add(b)
                stack.append(b)
    return seen


def gn_dir(base, d):
    """An include dir of a command of the build dir as gn spells it."""
    n = base["paths"].normalize(d)
    if n in base["paths"].source_roots:
        return "//"
    if n.startswith("<out>/"):
        return "$root_build_dir/" + n[len("<out>/"):]
    return n if os.path.isabs(n) or n.startswith("<") else "//" + n


def write(args, base):
    """Writes //build/gn/pch: groups.gni, a <name>/pch.h and pch.cc per group;
    removes the dirs of the groups gone."""
    m, found = base["model"], base["found"]
    names, objs = base["header_names"], base["objs"]
    all_targets = set(base["target_of"].values())
    first = base["class_flags"][0]
    universal = set(base["defines_of"][objs[0]])
    universal_dirs = set(base["include_dirs_of"][objs[0]])
    for o in objs[1:]:
        universal.intersection_update(base["defines_of"][o])
        universal_dirs.intersection_update(base["include_dirs_of"][o])
    producer, deps, exe = target_graph(base)
    # the generator of a library is private: its <name>_codegen group is not
    codegen = {}
    for a, bs in deps.items():
        if a.endswith("_codegen"):
            for b in bs:
                if b.partition(":")[0] == a.partition(":")[0]:
                    codegen.setdefault(b, a)
    problems = collections.defaultdict(list)

    written = []                                # (sel, members, header, deps, cflags, defines, include_dirs)
    out_for_cycles = 0
    for n, sel in [g[:2] for g in found]:
        value = sel.evaluate(sel.parse, sel.build, len(sel.hit), sel.hit_c, sel.nonhit_p, sel.nonhit_n, sel.ready)[0]
        if value < _MIN_GROUP_G:
            continue
        flags = base["class_flags"][n - 1]
        lost = [f for f in first if f not in flags]
        if lost:
            problems["groups of a class lacking flags of class 1"].append("%d: %s" % (n, " ".join(lost)))
            continue
        _, roots, defines, include_dirs = group_inputs(base, sel)
        if not roots:
            problems["groups with no header"].append(str(n))
            continue
        gens = set()
        for h in range(len(names)):
            if sel.selected[m["comp"][h]] and names[h].startswith("<out>/"):
                t = producer.get(names[h][len("<out>/"):])
                if t is None or not t.startswith("//"):
                    problems["generated headers made by no target of the manifest"].append(names[h])
                else:
                    gens.add(codegen.get(t, t))
        members = set(sel.members) - reachable(deps, exe, gens | set(_PCH_DEPS))
        out_for_cycles += len(sel.members) - len(members)
        if not members:
            continue
        cflags = [a for f in flags if f not in first for a in f.split(" ")]
        cflags = [gn_escape(a) for a in cflags] + [d for d in defines if d.startswith("-U") and d not in universal]
        written.append((value, sel, members, roots, sorted(gens), cflags,
                        [gn_escape(d[2:]) for d in defines if d.startswith("-D") and d not in universal],
                        [gn_dir(base, d) for opt, d in include_dirs if (opt, d) not in universal_dirs],
                        (list(defines), [gn_dir(base, d) for _, d in include_dirs])))
    written.sort(key=lambda w: -w[0])

    # the names of the groups now: contrib, contrib_2...; the others by the most targets in common
    before = dict((name, pats) for name, pats in gni_lists(os.path.join(_ROOT, _GROUPS_GNI), "targets")
                  if not (name or "").startswith(_CONTRIB))
    taken, numbered = set(), []
    for w in written:
        if is_contrib(base, w[1]):
            k = sum(1 for name in numbered if name and name.startswith(_CONTRIB))
            numbered.append(_CONTRIB + ("_%d" % (k + 1) if k else ""))
            continue
        common = [(sum(1 for t in w[2] if label_matches(t, pats)), name) for name, pats in before.items()
                  if name not in taken]
        best = max(common, default=(0, None))
        numbered.append(best[1] if best[0] else None)
        if best[0]:
            taken.add(best[1])
    k = 1
    for i, name in enumerate(numbered):
        while name is None and str(k) in taken:
            k += 1
        if name is None:
            numbered[i] = name = str(k)
            taken.add(name)

    pch_dir = os.path.join(_ROOT, _PCH_DIR)
    text = ["# %s\n" % _WRITTEN, "pch_groups = [\n"]
    patterns = 0
    base["written"], base["written_inputs"] = {}, {}
    for name, (value, sel, members, roots, gens, cflags, defines, include_dirs, checked) in zip(numbered, written):
        pats = fold(members, all_targets)
        patterns += len(pats)
        base["written"][name] = members
        base["written_inputs"][name] = checked  # what the checks built the PCH with: all its -D, -U and dirs
        text += ["  {\n", '    name = "%s"\n' % name, gn_list("targets", pats, 4), gn_list("deps", gens, 4),
                 gn_list("cflags", cflags, 4), gn_list("defines", defines, 4),
                 gn_list("include_dirs", include_dirs, 4), "  },\n"]
        os.makedirs(os.path.join(pch_dir, name), exist_ok=True)
        with open(os.path.join(pch_dir, name, "pch.h"), "w", encoding="utf-8") as f:
            f.write("// %s\n" % _WRITTEN)
            for h in roots:
                f.write("#include %s\n" % spelling(base, names[h]))
        with open(os.path.join(pch_dir, name, "pch.cc"), "w", encoding="utf-8") as f:
            f.write('#include "pch.h"\n')
    text.append("]\n")
    with open(os.path.join(_ROOT, _GROUPS_GNI), "w", encoding="utf-8") as f:
        f.write("".join(text))
    print("write:            groups %d of %d (G %s+), targets %d in %d patterns, generator deps %d, "
          "targets out for cycles %d" % (
              len(written), len(found), duration(_MIN_GROUP_G), sum(len(w[2]) for w in written), patterns,
              len(set(t for w in written for t in w[4])), out_for_cycles))

    # the dirs of no group: but those of the PCH of pch_targets
    own = set(label[2:].split("/")[0] for _, labels in gni_lists(os.path.join(_ROOT, _TARGETS_GNI), "pch_targets")
              for label in labels)
    gone = sorted(d for d in os.listdir(pch_dir) if os.path.isdir(os.path.join(pch_dir, d))
                  and d not in base["written"] and d not in own)
    for d in gone:
        shutil.rmtree(os.path.join(pch_dir, d))
    print("cleanup:          removed %d dirs%s" % (len(gone), (": " + ", ".join(gone)) if gone else ""))
    for what, examples in sorted(problems.items()):
        print("  %d %s, e.g. %s" % (len(examples), what, display(examples[0])))
    return not problems


# =============================================================================
# gn gen
# =============================================================================

_PCH_MODE_RE = re.compile(r"^\s*pch_mode\s*=")
_OUTPUT_RE = re.compile(r"\s-o\s+(\S+)")
_INCLUDE_PCH_RE = re.compile(r"-include-pch\s+\S*/pch_([^/\s]+)\.pch")
_PCH_SOURCE_RE = re.compile(r"(?:^|/)%s/([^/]+)/pch\.cc$" % re.escape(_PCH_DIR))
_PCH_ONLY = ("-x c++-header", "-fpch-instantiate-templates")
_TOP_GN_ERROR = 10


def gn_gen(args, base):
    """gn gen of this tree with the args of --baseline but pch_mode =
    "global" in a temporary dir: each C++ file of the baseline has the PCH of
    the group written for its target, each PCH the flags of its files."""
    build_dir = base["build_dir"]
    with open(os.path.join(build_dir, "build.ninja"), encoding="utf-8") as f:
        m = re.search(r"^rule gn\n\s*command = (.*)$", f.read(), re.M)
    argv = m.group(1).split() if m else []
    gn = os.path.normpath(os.path.join(build_dir, argv[0])) if argv else "gn"
    extra = [a for a in argv if a.startswith("--script-executable")]
    out = os.path.realpath(tempfile.mkdtemp(prefix="pch_update.gn."))
    try:
        with open(os.path.join(build_dir, "args.gn"), encoding="utf-8") as f:
            lines = [line for line in f if not _PCH_MODE_RE.match(line)]
        with open(os.path.join(out, "args.gn"), "w", encoding="utf-8") as f:
            f.write("".join(lines) + 'pch_mode = "global"\n')
        start = time.time()
        r = subprocess.run([gn, "--root=" + _ROOT, "-q"] + extra + ["gen", out], stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT, universal_newlines=True, errors="replace")
        took = (time.time() - start) * 10**6
        if r.returncode:
            print("gn gen:           failed in %s:" % duration(took))
            for line in r.stdout.strip().splitlines()[:_TOP_GN_ERROR]:
                print("  " + line)
            return False
        with open(os.path.join(out, "compile_commands.json"), encoding="utf-8") as f:
            commands = json.load(f)
    finally:
        shutil.rmtree(out, ignore_errors=True)

    def spelled(d):
        """An include dir of a command of `out` as gn spells it."""
        p = os.path.normpath(os.path.join(out, d))
        if p.startswith(out + "/"):
            return "$root_build_dir/" + p[len(out) + 1:]
        if p == _ROOT:
            return "//"
        return "//" + p[len(_ROOT) + 1:] if p.startswith(_ROOT + "/") else p

    group_of = dict((t, name) for name, ts in base["written"].items() for t in ts)
    target_of, compiler = base["target_of"], base["compiler"]
    pch_flags, files = {}, {}                   # group -> (flags, -D); obj -> (pch, flags, -D)
    inputs_bad = {}                             # group -> what of groups.gni its PCH lacks
    for c in commands:
        command = c["command"]
        if compiler not in command:
            continue
        m = _PCH_SOURCE_RE.search(c["file"])
        flags, defines, include_dirs = compile_flags(command, compiler)[:3]
        flags = tuple(f for f in flags if f.startswith("-") and f != "-include-pch")
        if m:
            pch_flags[m.group(1)] = (tuple(sorted(f for f in flags if f not in _PCH_ONLY)), set(defines))
            want_d, want_i = base["written_inputs"].get(m.group(1), ([], []))
            lack = ([d for d in want_d if d not in set(defines)] +
                    [d for d in want_i if d not in set(spelled(x) for _, x in include_dirs)])
            if lack:
                inputs_bad[m.group(1)] = lack
            continue
        o = _OUTPUT_RE.search(command)
        if o and o.group(1) in target_of:
            p = _INCLUDE_PCH_RE.search(command)
            files[o.group(1)] = (p.group(1) if p else None, tuple(sorted(flags)), set(defines))

    wrong = collections.defaultdict(list)       # target -> (file, predicted, got)
    flags_bad = collections.defaultdict(list)   # group -> (file, what)
    no_hook, elsewhere = set(), set()
    for obj, target in target_of.items():
        if obj not in files:
            elsewhere.add(target)               # a tool: built in tool_toolchain only
            continue
        pch, flags, defines = files[obj]
        if pch is None and group_of.get(target) is not None:
            no_hook.add(target)                 # its template takes no PCH (static_library ...)
        elif pch != group_of.get(target):
            wrong[target].append((obj, group_of.get(target), pch))
        elif pch is not None and pch in pch_flags:
            want, want_d = pch_flags[pch]
            if flags != want:
                flags_bad[pch].append((obj, " ".join(["+" + f for f in flags if f not in want] +
                                                     ["-" + f for f in want if f not in flags])))
            elif not want_d <= defines:
                flags_bad[pch].append((obj, "PCH -D not here: " + " ".join(sorted(want_d - defines))))
    targets = set(target_of.values())
    print("gn gen:           ok in %s; PCH as predicted %d of %d targets (no PCH by template %d, tools %d); "
          "PCH flags match its files %d of %d groups, -D and include dirs of the checks %d of %d" % (
              duration(took), len(targets) - len(wrong), len(targets), len(no_hook - set(wrong)),
              len(elsewhere - set(wrong) - no_hook), sum(1 for g in base["written"] if g in pch_flags and g not in flags_bad),
              len(base["written"]), sum(1 for g in base["written"] if g in pch_flags and g not in inputs_bad),
              len(base["written"])))
    for target, bad in sorted(wrong.items())[:_TOP_GN_ERROR]:
        obj, want, got = bad[0]
        print("  %s: %s has PCH %s, predicted %s" % (target, display(obj), got, want))
    for g in sorted(set(base["written"]) - set(pch_flags)):
        print("  group %s: no compile of its PCH" % g)
    for g, bad in sorted(flags_bad.items()):
        print("  group %s: %d files differ, e.g. %s: %s" % (g, len(bad), display(bad[0][0]), bad[0][1]))
    for g, lack in sorted(inputs_bad.items()):
        print("  group %s: PCH lacks %d of the -D and include dirs the checks built it with, e.g. %s" % (g, len(lack), lack[0]))
    return not wrong and not flags_bad and not inputs_bad and set(pch_flags) >= set(base["written"])


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--baseline", required=True, help="build dir with pch_mode = \"none\", time_trace = true")
    ap.add_argument("--targets", nargs="+", default=["all", "tests"], help="what was built there")
    ap.add_argument("--max-missing-traces", type=float, default=0.01,
                    help="share of the C++ objects built that may lack a trace")
    ap.add_argument("-j", "--jobs", type=int, default=min(8, multiprocessing.cpu_count()),
                    help="processes reading the traces")
    ap.add_argument("--check-jobs", type=int, default=min(24, multiprocessing.cpu_count()),
                    help="compilers checking the PCH")
    ap.add_argument("--main-ref", default="origin/main", help="main, for the merge base of HEAD")
    ap.add_argument("--cache-dir", default=os.path.join(os.environ.get("XDG_CACHE_HOME") or
                                                        os.path.expanduser("~/.cache"), "pch_update"),
                    help="results of earlier runs (the headers compiled alone)")
    args = ap.parse_args()
    ok, base = check(args)
    if ok:
        ok = traces(args, base) and history(args, base) and flag_classes(args, base) \
            and header_facts(args, base) and groups(args, base) \
            and checks(args, base) and write(args, base) and gn_gen(args, base)
    print("OK" if ok else "FAILED")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
