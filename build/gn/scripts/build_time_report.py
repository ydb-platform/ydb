#!/usr/bin/env python3
"""Aggregate build time statistics of a gn/ninja build.

Build with clang time traces first:

    # args.gn
    time_trace = true

    ninja -C <out> -j<N> <targets>
    python3 build/gn/scripts/build_time_report.py <out> [-o <report dir>]

Sources of data:
  * <out>/.ninja_log   - wall time of every command (compile, link, codegen actions);
  * <out>/.ninja_deps  - header dependencies of every object;
  * <out>/**/*.ninja   - build graph, rules, gn target of every command;
  * <out>/obj/**/*.json - clang -ftime-trace of every object (gzipped *.json.gz are read too).

Report (<report dir>/report.md plus CSV tables with full data):
  * overview: time by kind of command, clang frontend/backend split;
  * slowest translation units and gn targets;
  * expensive headers: parse time summed over all TUs, and rebuild cost
    (compile time of all objects depending on the header);
  * expensive templates (by template family and by exact instantiation)
    and functions in code generation / optimization;
  * build graph bottlenecks: critical path (with unlimited parallelism)
    and long commands that many other commands wait for;
  * precompiled header candidates: per gn target and a common set of headers,
    with the estimated saving.

All times of a single clang event are inclusive (nested events are included),
so the sums in different tables overlap and must not be added up.
"""

import argparse
import array
import collections
import csv
import gzip
import json
import multiprocessing
import os
import re
import struct
import sys


# =============================================================================
# Paths
# =============================================================================

class Paths(object):
    def __init__(self, build_dir, source_root, sysroot):
        self.build_dir = os.path.realpath(build_dir)
        self.source_root = os.path.realpath(source_root) if source_root else None
        self.sysroot = os.path.realpath(sysroot) if sysroot else None
        self._cache = {}

    def normalize(self, path):
        """Source-relative path for sources, gen/... for generated files, <sysroot>/... for sysroot."""
        result = self._cache.get(path)
        if result is not None:
            return result
        p = path if os.path.isabs(path) else os.path.join(self.build_dir, path)
        p = os.path.normpath(p)
        if self.source_root and p.startswith(self.source_root + os.sep):
            result = p[len(self.source_root) + 1:]
        elif p.startswith(self.build_dir + os.sep):
            result = "<out>/" + p[len(self.build_dir) + 1:]
        elif self.sysroot and p.startswith(self.sysroot + os.sep):
            result = "<sysroot>/" + p[len(self.sysroot) + 1:]
        else:
            result = p
        self._cache[path] = result
        return result


def detect_roots(build_dir):
    source_root = None
    sysroot = None
    try:
        with open(os.path.join(build_dir, "build.ninja")) as f:
            m = re.search(r"--root=(\S+)", f.read())
            if m:
                source_root = os.path.join(build_dir, m.group(1))
    except IOError:
        pass
    try:
        with open(os.path.join(build_dir, "toolchain.ninja")) as f:
            m = re.search(r"--sysroot=(\S+)", f.read())
            if m:
                sysroot = m.group(1)
    except IOError:
        pass
    return source_root, sysroot


# =============================================================================
# Ninja manifest
# =============================================================================

Edge = collections.namedtuple("Edge", "rule outputs inputs order_only target bindings scope")


class Scope(object):
    """Variables of a ninja file; subninja gets a child scope, include shares the scope."""

    def __init__(self, parent=None):
        self.vars = {}
        self.parent = parent

    def lookup(self, name):
        scope = self
        while scope is not None:
            value = scope.vars.get(name)
            if value is not None:
                return value
            scope = scope.parent
        return ""


_var_ref = re.compile(r"\$(\$|:| |\{([A-Za-z0-9_.-]+)\}|([A-Za-z0-9_-]+))")


def expand(text, lookup):
    if "$" not in text:
        return text

    def repl(m):
        name = m.group(2) or m.group(3)
        return lookup(name) if name else m.group(1)
    return _var_ref.sub(repl, text)


def edge_command(edge, rules):
    """
    Command of the edge as ninja runs it, with file names replaced by placeholders:
    two edges with equal results are compiled with exactly the same flags.
    File names are $in, $out and gn per-source substitutions ({{source_name_part}} and the like,
    written as edge bindings source_*); all flags come from target and toolchain scopes.
    """
    rule = rules.get(edge.rule, {})

    def lookup(name):
        if name in ("in", "in_newline"):
            return "<in>"
        if name == "out":
            return "<out>"
        if name.startswith("source"):
            return "<%s>" % name
        if name in edge.bindings:
            return edge.bindings[name]
        if name in rule:
            return expand(rule[name], lookup)
        return edge.scope.lookup(name)
    # only the compiler invocation: post-processing like "&& gzip <trace>" doesn't affect the object
    return expand(rule.get("command", ""), lookup).split(" && ")[0]


def _norm(path):
    # ./libfoo.so -> libfoo.so, as in .ninja_log
    return os.path.normpath(path) if path.startswith(".") or "/." in path else path


def _split_ninja_paths(text):
    """Split a part of a build line into normalized paths, handling $-escapes."""
    return [_norm(p) for p in _split_escaped(text)]


def _split_escaped(text):
    if "$" not in text:
        return text.split()
    result = []
    current = []
    i = 0
    n = len(text)
    while i < n:
        c = text[i]
        if c == "$" and i + 1 < n:
            current.append(text[i + 1])
            i += 2
            continue
        if c == " ":
            if current:
                result.append("".join(current))
                current = []
        else:
            current.append(c)
        i += 1
    if current:
        result.append("".join(current))
    return result


def _find_unescaped(text, sub, start=0):
    i = text.find(sub, start)
    while i != -1:
        dollars = 0
        j = i - 1
        while j >= 0 and text[j] == "$":
            dollars += 1
            j -= 1
        if dollars % 2 == 0:
            return i
        i = text.find(sub, i + 1)
    return -1


def _target_of_ninja_file(rel_path):
    # obj/ydb/core/kqp/common/common.ninja -> //ydb/core/kqp/common:common
    if rel_path.startswith("obj/") and rel_path.endswith(".ninja"):
        d, name = os.path.split(rel_path[len("obj/"):-len(".ninja")])
        return "//%s:%s" % (d, name)
    return rel_path


def parse_ninja_manifest(build_dir):
    edges = []
    rules = {}
    visited = set()

    def parse_file(rel_path, scope):
        if rel_path in visited:
            return
        visited.add(rel_path)
        try:
            with open(os.path.join(build_dir, rel_path)) as f:
                raw = f.read()
        except IOError:
            return
        # line continuations
        raw = re.sub(r"(?<!\$)((?:\$\$)*)\$\n[ ]*", r"\1", raw)
        target = _target_of_ninja_file(rel_path)
        current_rule = None
        current_bindings = None
        for line in raw.split("\n"):
            if not line or line.startswith("#"):
                continue
            if line[0] == " ":
                key, _, value = line.strip().partition(" = ")
                if current_rule is not None:
                    # rule variables are expanded when the command runs
                    rules[current_rule][key] = value
                elif current_bindings is not None:
                    current_bindings[key.strip()] = expand(value, scope.lookup)
                continue
            current_rule = None
            current_bindings = None
            if line.startswith("build "):
                colon = _find_unescaped(line, ":", 6)
                outs = line[6:colon]
                rest = line[colon + 1:]
                pos = _find_unescaped(outs, "|")
                outputs = _split_ninja_paths(outs if pos == -1 else outs[:pos] + " " + outs[pos + 1:])
                validations = _find_unescaped(rest, "|@")
                if validations != -1:
                    rest = rest[:validations]
                order_only = []
                pos = _find_unescaped(rest, "||")
                if pos != -1:
                    order_only = _split_ninja_paths(rest[pos + 2:])
                    rest = rest[:pos]
                pos = _find_unescaped(rest, "|")
                if pos != -1:
                    rest = rest[:pos] + " " + rest[pos + 1:]
                parts = _split_escaped(rest)
                current_bindings = {}
                edges.append(Edge(parts[0], outputs, [_norm(p) for p in parts[1:]], order_only, target,
                                  current_bindings, scope))
            elif line.startswith("rule "):
                current_rule = line[5:].strip()
                rules[current_rule] = {}
            elif line.startswith("subninja "):
                parse_file(line.split(None, 1)[1].strip(), Scope(scope))
            elif line.startswith("include "):
                parse_file(line.split(None, 1)[1].strip(), scope)
            elif " = " in line and not line.startswith(("pool ", "default ")):
                key, _, value = line.partition(" = ")
                scope.vars[key.strip()] = expand(value, scope.lookup)

    parse_file("build.ninja", Scope())
    return edges, rules


def rule_kind(rule_name, rules):
    """Human-readable kind of a command: cxx, solink, action:protoc.py, ..."""
    if not rule_name.startswith("__"):
        return rule_name
    command = rules.get(rule_name, {}).get("command", "")
    tokens = command.split()
    for i, token in enumerate(tokens):
        if token.endswith(".py"):
            name = os.path.basename(token)
            if name in ("run.py", "run_binary.py", "run_in_dir.py") and i + 1 < len(tokens):
                return "action:%s %s" % (name, os.path.basename(tokens[i + 1]))
            return "action:" + name
    return "action:" + (os.path.basename(tokens[0]) if tokens else "?")


# =============================================================================
# Ninja log and deps
# =============================================================================

def parse_ninja_log(build_dir):
    """
    output -> (start_ms, end_ms) of the latest command, and the time span of the log in ms.
    Times are relative to the start of each ninja run, so the span is the build wall time only
    for a log of a single run (e.g. a build from scratch).
    """
    result = {}
    span_start, span_end = None, 0
    with open(os.path.join(build_dir, ".ninja_log")) as f:
        header = f.readline()
        if not header.startswith("# ninja log"):
            raise RuntimeError("unknown .ninja_log format")
        for line in f:
            fields = line.rstrip("\n").split("\t")
            if len(fields) < 4:
                continue
            start, end = int(fields[0]), int(fields[1])
            span_start = start if span_start is None else min(span_start, start)
            span_end = max(span_end, end)
            result[os.path.normpath(fields[3])] = (start, end)
    return result, span_end - (span_start or 0)


def parse_ninja_deps(build_dir):
    """(output -> array of path ids, list of paths) from the binary .ninja_deps (format version 3 and 4)."""
    with open(os.path.join(build_dir, ".ninja_deps"), "rb") as f:
        data = f.read()
    signature = b"# ninjadeps\n"
    if not data.startswith(signature):
        raise RuntimeError("unknown .ninja_deps format")
    version, = struct.unpack_from("<i", data, len(signature))
    if version not in (3, 4):
        raise RuntimeError("unsupported .ninja_deps version %d" % version)
    mtime_size = 8 if version == 4 else 4
    offset = len(signature) + 4
    paths = []
    deps = {}
    n = len(data)
    while offset + 4 <= n:
        size, = struct.unpack_from("<I", data, offset)
        offset += 4
        is_deps = size & 0x80000000
        size &= 0x7FFFFFFF
        if offset + size > n:
            break
        if is_deps:
            out_id, = struct.unpack_from("<i", data, offset)
            ids = array.array("i")
            ids.frombytes(data[offset + 4 + mtime_size:offset + size])
            deps[out_id] = ids
        else:
            paths.append(data[offset:offset + size - 4].rstrip(b"\0").decode("utf-8", "replace"))
        offset += size
    return dict((paths[k], v) for k, v in deps.items() if k < len(paths)), paths


# =============================================================================
# Clang time traces
# =============================================================================

TEMPLATE_EVENTS = ("InstantiateClass", "InstantiateFunction")
FUNCTION_EVENTS = ("CodeGen Function", "OptFunction", "DebugType", "ParseClass")
TOP_DETAILS_PER_TU = 100

_template_args = re.compile(r"<[^<>]*>")


def template_family(name):
    """NFoo::TBar<int, TBaz<char>>::Do<long> -> NFoo::TBar<>::Do<>"""
    if "<" not in name:
        return name
    prev = None
    s = name
    while s != prev:
        prev = s
        s = _template_args.sub("\x01", s)
    if "<" in s or ">" in s:
        # operator< and the like, can't strip reliably
        return name
    return s.replace("\x01", "<>")


_worker_paths = None


def _init_worker(paths):
    global _worker_paths
    _worker_paths = paths


def _open_trace(path):
    if path.endswith(".gz"):
        return gzip.open(path, "rt")
    return open(path)


def load_trace(job):
    """Summary of one TU trace, small enough to send between processes."""
    obj, trace_path = job
    try:
        with _open_trace(trace_path) as f:
            events = json.load(f)["traceEvents"]
    except Exception as e:
        return obj, None, str(e)

    totals = {}
    header_names = []
    header_index = {}
    h_ids = array.array("i")
    h_ts = array.array("q")
    h_dur = array.array("q")
    families = collections.defaultdict(lambda: [0, 0])
    details = collections.defaultdict(lambda: collections.defaultdict(lambda: [0, 0]))
    execute = 0

    for e in events:
        if e.get("ph") != "X":
            continue
        name = e["name"]
        dur = e.get("dur", 0)
        if name.startswith("Total "):
            totals[name[6:]] = dur
            continue
        if name == "ExecuteCompiler":
            execute = max(execute, dur)
        elif name == "Source":
            path = _worker_paths.normalize(e["args"]["detail"])
            idx = header_index.get(path)
            if idx is None:
                idx = header_index[path] = len(header_names)
                header_names.append(path)
            h_ids.append(idx)
            h_ts.append(e["ts"])
            h_dur.append(dur)
        elif name in TEMPLATE_EVENTS or name in FUNCTION_EVENTS:
            detail = e.get("args", {}).get("detail")
            if not detail:
                continue
            d = details[name][detail]
            d[0] += dur
            d[1] += 1
            if name in TEMPLATE_EVENTS:
                fam = families[template_family(detail)]
                fam[0] += dur
                fam[1] += 1

    if "ExecuteCompiler" not in totals:
        totals["ExecuteCompiler"] = execute

    top_details = {}
    for kind, values in details.items():
        top = sorted(values.items(), key=lambda kv: -kv[1][0])[:TOP_DETAILS_PER_TU]
        top_details[kind] = dict((k, tuple(v)) for k, v in top)

    return obj, {
        "totals": totals,
        "headers": (header_names, h_ids.tobytes(), h_ts.tobytes(), h_dur.tobytes()),
        "families": dict((k, tuple(v)) for k, v in families.items()),
        "details": top_details,
    }, None


class TU(object):
    __slots__ = ("obj", "target", "rule", "flags", "wall", "totals", "h_ids", "h_ts", "h_dur")


class HeaderTable(object):
    def __init__(self):
        self.names = []
        self.index = {}

    def id(self, name):
        i = self.index.get(name)
        if i is None:
            i = self.index[name] = len(self.names)
            self.names.append(name)
        return i


def trace_path_for(build_dir, obj):
    base = os.path.join(build_dir, obj[:-2] if obj.endswith(".o") else obj)
    for candidate in (base + ".json.gz", base + ".json"):
        if os.path.exists(candidate):
            return candidate
    return None


# =============================================================================
# Analyses
# =============================================================================

def union_length(intervals):
    """Total length of union of (start, end) intervals."""
    total = 0
    cur_start = cur_end = None
    for start, end in sorted(intervals):
        if cur_end is None or start > cur_end:
            if cur_end is not None:
                total += cur_end - cur_start
            cur_start, cur_end = start, end
        elif end > cur_end:
            cur_end = end
    if cur_end is not None:
        total += cur_end - cur_start
    return total


def outermost_events(tu, selected=None):
    """Indices of header events not nested into another (selected) header event."""
    order = sorted(range(len(tu.h_ids)), key=lambda i: (tu.h_ts[i], -tu.h_dur[i]))
    result = []
    open_end = -1
    for i in order:
        if selected is not None and tu.h_ids[i] not in selected:
            continue
        start = tu.h_ts[i]
        if start >= open_end:
            result.append(i)
            open_end = start + tu.h_dur[i]
        # else nested into the current outermost event
    return result


def pch_estimate(tus, selected, load_cost):
    """
    Estimated saving from a PCH with the `selected` headers for one gn target:
    parse time of the selected headers in every TU (union, nested counted once),
    minus the cost of loading the PCH in every TU, minus building the PCH once.
    """
    unions = []
    roots = collections.defaultdict(lambda: [0, 0])
    for tu in tus:
        idx = outermost_events(tu, selected)
        unions.append(sum(tu.h_dur[i] for i in idx))
        for i in idx:
            r = roots[tu.h_ids[i]]
            r[0] += tu.h_dur[i]
            r[1] += 1
    if not unions:
        return 0, 0, roots
    parse = sum(unions)
    saving = parse * (1.0 - load_cost) - max(unions)
    return saving, parse, roots


def critical_path(edges, durations):
    producer = {}
    for i, e in enumerate(edges):
        for o in e.outputs:
            producer[o] = i
    deps = []
    for e in edges:
        d = set()
        for p in e.inputs:
            j = producer.get(p)
            if j is not None:
                d.add(j)
        for p in e.order_only:
            j = producer.get(p)
            if j is not None:
                d.add(j)
        deps.append(d)

    finish = [None] * len(edges)
    via = [None] * len(edges)
    for root in range(len(edges)):
        if finish[root] is not None:
            continue
        stack = [(root, False)]
        while stack:
            node, expanded = stack.pop()
            if finish[node] is not None:
                continue
            if not expanded:
                stack.append((node, True))
                for d in deps[node]:
                    if finish[d] is None:
                        stack.append((d, False))
                continue
            best = 0
            best_dep = None
            for d in deps[node]:
                f = finish[d]
                if f is None:
                    # dependency cycle guard
                    continue
                if f > best:
                    best, best_dep = f, d
            finish[node] = best + durations[node]
            via[node] = best_dep

    end = max(range(len(edges)), key=lambda i: finish[i]) if edges else None
    path = []
    while end is not None:
        path.append(end)
        end = via[end]
    path.reverse()
    return path, finish, deps


def transitive_dependents(start, reverse_deps):
    seen = set([start])
    stack = [start]
    while stack:
        node = stack.pop()
        for r in reverse_deps[node]:
            if r not in seen:
                seen.add(r)
                stack.append(r)
    return len(seen) - 1


# =============================================================================
# Output helpers
# =============================================================================

def fmt_s(us):
    return "%.1f" % (us / 1e6)


def fmt_h(us):
    s = us / 1e6
    if s >= 3600:
        return "%.1fh" % (s / 3600)
    if s >= 60:
        return "%.1fm" % (s / 60)
    return "%.1fs" % s


def md_table(out, header, rows):
    out.append("| " + " | ".join(header) + " |")
    out.append("|" + "---|" * len(header))
    for row in rows:
        out.append("| " + " | ".join(str(c).replace("|", "\\|") for c in row) + " |")
    out.append("")


def short(text, limit=160):
    return text if len(text) <= limit else text[:limit - 3] + "..."


def write_csv(report_dir, name, header, rows):
    with open(os.path.join(report_dir, name), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("build_dir")
    parser.add_argument("-o", "--report-dir",
                        help="default: build_time_report next to the build dir (out of reach of `gn clean`)")
    parser.add_argument("-j", "--jobs", type=int, default=multiprocessing.cpu_count())
    parser.add_argument("--source-root", help="default: from build.ninja")
    parser.add_argument("--traces-root", help="directory mirroring obj/ with traces, default: build_dir")
    parser.add_argument("--top", type=int, default=50, help="rows in markdown tables")
    parser.add_argument("--pch-min-tus", type=int, default=4, help="skip targets with fewer C++ TUs")
    parser.add_argument("--pch-coverage", type=float, default=0.75,
                        help="header goes to the target PCH if included by this share of its TUs")
    parser.add_argument("--pch-global-coverage", type=float, default=0.25,
                        help="header goes to the common PCH if included by this share of all C++ TUs")
    parser.add_argument("--pch-load-cost", type=float, default=0.15,
                        help="cost of loading a PCH as a share of parsing its headers")
    parser.add_argument("--pch-include-own", action="store_true",
                        help="consider headers from the target's own directory (unstable, usually edited together)")
    args = parser.parse_args()

    build_dir = os.path.realpath(args.build_dir)
    report_dir = args.report_dir or os.path.join(os.path.dirname(os.path.realpath(build_dir)), "build_time_report")
    traces_root = args.traces_root or build_dir
    if not os.path.isdir(report_dir):
        os.makedirs(report_dir)

    source_root, sysroot = detect_roots(build_dir)
    paths = Paths(build_dir, args.source_root or source_root, sysroot)

    def log(msg):
        sys.stderr.write(msg + "\n")
        sys.stderr.flush()

    log("parsing ninja manifest...")
    edges, rules = parse_ninja_manifest(build_dir)
    log("  %d edges, %d rules" % (len(edges), len(rules)))

    log("parsing .ninja_log...")
    ninja_log, span = parse_ninja_log(build_dir)
    durations = []
    for e in edges:
        d = 0
        for o in e.outputs:
            t = ninja_log.get(o)
            if t:
                d = max(d, (t[1] - t[0]) * 1000)
        durations.append(d)
    kinds = [rule_kind(e.rule, rules) for e in edges]

    log("parsing .ninja_deps...")
    try:
        ninja_deps, dep_paths = parse_ninja_deps(build_dir)
    except (IOError, RuntimeError) as err:
        log("  skipped: %s" % err)
        ninja_deps, dep_paths = {}, []

    # ---- traces
    compile_edges = [i for i, e in enumerate(edges) if e.rule in ("cxx", "cc") and e.outputs]
    jobs = []
    for i in compile_edges:
        obj = edges[i].outputs[0]
        p = trace_path_for(traces_root, obj)
        if p:
            jobs.append((i, p))
    log("loading %d clang traces with %d processes..." % (len(jobs), args.jobs))

    headers = HeaderTable()
    tus = []
    phase_totals = collections.Counter()
    families = collections.defaultdict(lambda: [0, 0, 0])  # dur, count, tus
    details = collections.defaultdict(lambda: collections.defaultdict(lambda: [0, 0, 0]))
    errors = 0

    pool = multiprocessing.Pool(args.jobs, initializer=_init_worker, initargs=(paths,))
    try:
        for n, (edge_idx, summary, error) in enumerate(pool.imap_unordered(load_trace, jobs, chunksize=8)):
            if n % 1000 == 0:
                log("  %d/%d" % (n, len(jobs)))
            if summary is None:
                errors += 1
                continue
            tu = TU()
            tu.obj = edges[edge_idx].outputs[0]
            tu.target = edges[edge_idx].target
            tu.rule = edges[edge_idx].rule
            tu.flags = sys.intern(edge_command(edges[edge_idx], rules))
            tu.wall = durations[edge_idx]
            tu.totals = summary["totals"]
            names, ids_b, ts_b, dur_b = summary["headers"]
            local = array.array("i")
            local.frombytes(ids_b)
            tu.h_ids = array.array("i", (headers.id(names[k]) for k in local))
            tu.h_ts = array.array("q")
            tu.h_ts.frombytes(ts_b)
            tu.h_dur = array.array("q")
            tu.h_dur.frombytes(dur_b)
            tus.append(tu)
            phase_totals.update(tu.totals)
            for k, (d, c) in summary["families"].items():
                f = families[k]
                f[0] += d
                f[1] += c
                f[2] += 1
            for kind, values in summary["details"].items():
                target = details[kind]
                for k, (d, c) in values.items():
                    v = target[k]
                    v[0] += d
                    v[1] += c
                    v[2] += 1
    finally:
        pool.close()
        pool.join()
    if errors:
        log("  %d traces failed to load" % errors)

    out = []
    top = args.top

    out.append("# Build time report")
    out.append("")
    out.append("Build dir: `%s`, source root: `%s`." % (build_dir, paths.source_root))
    out.append("")
    out.append("All clang event times are inclusive: nested events are counted in their parents, "
               "so rows of one table overlap. `wall` is the time from .ninja_log, clang times are wall-clock "
               "inside the compiler process; both depend on machine load, compare builds made with the same -j.")
    out.append("")

    # ---- overview
    out.append("## Overview")
    out.append("")
    out.append("Build wall time: %s (valid only if .ninja_log holds a single ninja run, e.g. a build from scratch). "
               "Commands with timing: %d, clang traces: %d."
               % (fmt_h(span * 1000), sum(1 for d in durations if d), len(tus)))
    out.append("")
    by_kind = collections.defaultdict(lambda: [0, 0, 0])
    for k, d in zip(kinds, durations):
        if d:
            v = by_kind[k]
            v[0] += 1
            v[1] += d
            v[2] = max(v[2], d)
    rows = sorted(by_kind.items(), key=lambda kv: -kv[1][1])
    total_cmd = sum(v[1] for v in by_kind.values()) or 1
    md_table(out, ["kind", "commands", "total", "share", "max"],
             [(k, v[0], fmt_h(v[1]), "%.1f%%" % (100.0 * v[1] / total_cmd), fmt_h(v[2])) for k, v in rows[:top]])
    write_csv(report_dir, "kinds.csv", ["kind", "commands", "total_s", "max_s"],
              [(k, v[0], fmt_s(v[1]), fmt_s(v[2])) for k, v in rows])

    if tus:
        out.append("### Clang activities (sum over all TUs)")
        out.append("")
        compile_total = phase_totals.get("ExecuteCompiler", 0) or 1
        rows = phase_totals.most_common()
        md_table(out, ["activity", "total", "share of ExecuteCompiler"],
                 [(k, fmt_h(v), "%.1f%%" % (100.0 * v / compile_total)) for k, v in rows[:40]])
        write_csv(report_dir, "activities.csv", ["activity", "total_s"], [(k, fmt_s(v)) for k, v in rows])

    # ---- TUs
    if tus:
        out.append("## Slowest translation units")
        out.append("")
        rows = sorted(tus, key=lambda t: -t.totals.get("ExecuteCompiler", 0))
        header = ["object", "target", "clang", "frontend", "headers", "templates", "backend", "wall"]

        def tu_row(t, fmt):
            g = t.totals.get
            return (t.obj, t.target, fmt(g("ExecuteCompiler", 0)), fmt(g("Frontend", 0)), fmt(g("Source", 0)),
                    fmt(g("InstantiateClass", 0) + g("InstantiateFunction", 0)), fmt(g("Backend", 0)), fmt(t.wall))
        md_table(out, header, [tu_row(t, fmt_h) for t in rows[:top]])
        write_csv(report_dir, "tus.csv", [h + ("_s" if i >= 2 else "") for i, h in enumerate(header)],
                  [tu_row(t, fmt_s) for t in rows])

    # ---- targets
    out.append("## Slowest gn targets")
    out.append("")
    targets = collections.defaultdict(lambda: [0, 0, 0, 0, 0])  # compile wall, tus, link wall, other, headers
    for e, k, d in zip(edges, kinds, durations):
        t = targets[e.target]
        if e.rule in ("cxx", "cc", "asm"):
            t[0] += d
            t[1] += 1
        elif e.rule in ("solink", "link", "alink"):
            t[2] += d
        else:
            t[3] += d
    for tu in tus:
        targets[tu.target][4] += tu.totals.get("Source", 0)
    rows = sorted(targets.items(), key=lambda kv: -(kv[1][0] + kv[1][2] + kv[1][3]))
    header = ["target", "compile wall", "objects", "link wall", "actions wall", "header parsing (clang)"]
    md_table(out, header, [(k, fmt_h(v[0]), v[1], fmt_h(v[2]), fmt_h(v[3]), fmt_h(v[4])) for k, v in rows[:top]])
    write_csv(report_dir, "targets.csv",
              ["target", "compile_wall_s", "objects", "link_wall_s", "actions_wall_s", "header_parsing_s"],
              [(k, fmt_s(v[0]), v[1], fmt_s(v[2]), fmt_s(v[3]), fmt_s(v[4])) for k, v in rows])

    # ---- headers
    header_stats = None
    if tus:
        log("aggregating headers...")
        # inclusive, TU count, outermost (included directly or first seen at top level)
        header_stats = collections.defaultdict(lambda: [0, 0, 0, 0])
        for tu in tus:
            seen = set()
            for h, d in zip(tu.h_ids, tu.h_dur):
                s = header_stats[h]
                s[0] += d
                if h not in seen:
                    seen.add(h)
                    s[1] += 1
            for i in outermost_events(tu):
                s = header_stats[tu.h_ids[i]]
                s[2] += tu.h_dur[i]
                s[3] += 1
        out.append("## Expensive headers")
        out.append("")
        out.append("`parse total` is the inclusive time of the header (with everything it includes) summed over TUs "
                   "where it was parsed. `as outermost` counts only occurrences not nested into another header, "
                   "i.e. the time the header really adds when included first. Headers parsed in less than "
                   "time_trace_granularity are not recorded.")
        out.append("")
        rows = sorted(header_stats.items(), key=lambda kv: -kv[1][0])
        header = ["header", "parse total", "TUs", "avg", "as outermost", "outermost TUs"]
        md_table(out, header,
                 [(headers.names[h], fmt_h(v[0]), v[1], "%.2fs" % (v[0] / 1e6 / max(v[1], 1)), fmt_h(v[2]), v[3])
                  for h, v in rows[:top]])
        write_csv(report_dir, "headers.csv",
                  ["header", "parse_total_s", "tus", "avg_s", "outermost_s", "outermost_tus"],
                  [(headers.names[h], fmt_s(v[0]), v[1], "%.3f" % (v[0] / 1e6 / max(v[1], 1)), fmt_s(v[2]), v[3])
                   for h, v in rows])

    if ninja_deps:
        log("aggregating header rebuild cost...")
        cost = collections.defaultdict(lambda: [0, 0])
        compile_wall = dict((edges[i].outputs[0], durations[i]) for i in compile_edges)
        # the same header may be spelled differently, e.g. absolute paths from a precompiled header
        dep_names = {}
        for obj, dep_list in ninja_deps.items():
            d = compile_wall.get(obj)
            if d is None:
                continue
            names = set()
            for dep in dep_list:
                name = dep_names.get(dep)
                if name is None:
                    name = dep_names[dep] = paths.normalize(dep_paths[dep])
                names.add(name)
            for name in names:
                c = cost[name]
                c[0] += d
                c[1] += 1
        rows = []
        for name, v in cost.items():
            # system and compiler headers never change, the sources themselves are not headers
            if os.path.isabs(name) or name.startswith("<sysroot>/") or name.endswith((".cpp", ".cc", ".c", ".cxx")):
                continue
            rows.append((name, v))
        rows.sort(key=lambda kv: -kv[1][0])
        out.append("## Header rebuild cost")
        out.append("")
        out.append("Wall time of compiling every object that depends on the header (from .ninja_deps): "
                   "what a touch of the header costs. Candidates for splitting, forward declarations, "
                   "moving code out of headers. System and compiler headers are skipped.")
        out.append("")
        md_table(out, ["header", "rebuild wall", "dependent objects"],
                 [(k, fmt_h(v[0]), v[1]) for k, v in rows[:top]])
        write_csv(report_dir, "header_rebuild_cost.csv", ["header", "rebuild_wall_s", "objects"],
                  [(k, fmt_s(v[0]), v[1]) for k, v in rows])

    # ---- templates and functions
    if tus:
        out.append("## Expensive templates")
        out.append("")
        out.append("Template family: all instantiations with template arguments erased. "
                   "Inclusive and summed over TUs; nested instantiations are counted in each level. "
                   "Candidates for `extern template`, type erasure, lighter metaprogramming.")
        out.append("")
        rows = sorted(families.items(), key=lambda kv: -kv[1][0])
        md_table(out, ["template family", "total", "instantiations", "TUs"],
                 [(short(k), fmt_h(v[0]), v[1], v[2]) for k, v in rows[:top]])
        write_csv(report_dir, "template_families.csv", ["family", "total_s", "instantiations", "tus"],
                  [(k, fmt_s(v[0]), v[1], v[2]) for k, v in rows])

        out.append("### Exact instantiations")
        out.append("")
        out.append("Only the top %d events of each kind per TU are collected." % TOP_DETAILS_PER_TU)
        out.append("")
        merged = []
        for kind in TEMPLATE_EVENTS:
            merged.extend((kind, k, v) for k, v in details.get(kind, {}).items())
        merged.sort(key=lambda r: -r[2][0])
        md_table(out, ["kind", "instantiation", "total", "count", "TUs"],
                 [(kind, short(k), fmt_h(v[0]), v[1], v[2]) for kind, k, v in merged[:top]])
        write_csv(report_dir, "template_instances.csv", ["kind", "instantiation", "total_s", "count", "tus"],
                  [(kind, k, fmt_s(v[0]), v[1], v[2]) for kind, k, v in merged])

        for kind, title, note in (
                ("CodeGen Function", "Code generation of functions",
                 "The same function in many TUs means an inline/template function emitted everywhere: "
                 "candidates for moving out of line or `extern template`."),
                ("OptFunction", "Optimization of functions", "Present in optimized builds only."),
                ("DebugType", "Debug info for types", ""),
                ("ParseClass", "Parsing of classes", "")):
            values = details.get(kind)
            if not values:
                continue
            rows = sorted(values.items(), key=lambda kv: -kv[1][0])
            out.append("## " + title)
            out.append("")
            if note:
                out.append(note)
                out.append("")
            md_table(out, ["name", "total", "count", "TUs"],
                     [(short(k), fmt_h(v[0]), v[1], v[2]) for k, v in rows[:top]])
            write_csv(report_dir, kind.lower().replace(" ", "_") + ".csv", ["name", "total_s", "count", "tus"],
                      [(k, fmt_s(v[0]), v[1], v[2]) for k, v in rows])

    # ---- build graph
    if any(durations):
        log("analyzing build graph...")
        path, finish, deps = critical_path(edges, durations)
        out.append("## Build graph bottlenecks")
        out.append("")
        out.append("### Critical path")
        out.append("")
        out.append("The longest chain of commands, each waiting for the previous one: the lower bound of "
                   "the build time with unlimited parallelism (%s). Long steps here are worth splitting "
                   "or removing from the dependency chain." % fmt_h(finish[path[-1]] if path else 0))
        out.append("")
        rows = [(edges[i].outputs[0], kinds[i], edges[i].target, fmt_h(durations[i]), fmt_h(finish[i]))
                for i in path if durations[i]]
        md_table(out, ["output", "kind", "target", "duration", "finished at"], rows)
        write_csv(report_dir, "critical_path.csv", ["output", "kind", "target", "duration_s", "finished_at_s"],
                  [(edges[i].outputs[0], kinds[i], edges[i].target, fmt_s(durations[i]), fmt_s(finish[i]))
                   for i in path])

        reverse = [[] for _ in edges]
        for i, d in enumerate(deps):
            for j in d:
                reverse[j].append(i)
        candidates = sorted(range(len(edges)), key=lambda i: -durations[i])[:max(top * 4, 200)]
        rows = []
        for i in candidates:
            if not durations[i]:
                continue
            dependents = transitive_dependents(i, reverse)
            rows.append((durations[i] * dependents, i, dependents))
        rows.sort(reverse=True)
        out.append("### Long commands blocking many others")
        out.append("")
        out.append("Among the %d longest commands: how many commands transitively wait for each. "
                   "A long codegen action or link at the root of a big subgraph delays everything behind it."
                   % len(candidates))
        out.append("")
        md_table(out, ["output", "kind", "target", "duration", "waiting commands"],
                 [(edges[i].outputs[0], kinds[i], edges[i].target, fmt_h(durations[i]), n)
                  for _, i, n in rows[:top]])
        write_csv(report_dir, "blocking_commands.csv", ["output", "kind", "target", "duration_s", "waiting"],
                  [(edges[i].outputs[0], kinds[i], edges[i].target, fmt_s(durations[i]), n) for _, i, n in rows])

    # ---- PCH
    if tus:
        log("estimating PCH candidates...")
        all_cxx = [tu for tu in tus if tu.rule == "cxx"]
        # flag group: TUs with byte-identical compiler command (file names aside), can share one PCH
        flag_groups = collections.defaultdict(list)
        units = collections.defaultdict(list)  # (target, flags) - what a gn precompiled_header can serve
        for tu in all_cxx:
            flag_groups[tu.flags].append(tu)
            units[(tu.target, tu.flags)].append(tu)
        flags_per_target = collections.Counter(target for target, _ in units)
        flag_group_no = dict((flags, n) for n, (flags, _) in enumerate(
            sorted(flag_groups.items(), key=lambda kv: -len(kv[1])), 1))

        out.append("## Precompiled header candidates")
        out.append("")
        out.append("A PCH can be used only by TUs compiled with exactly the same command line as the PCH itself. "
                   "TUs are grouped by their full compiler command from the ninja manifest (with $in, $out and "
                   "per-source file names replaced by placeholders); estimates never mix TUs of different groups. "
                   "Estimate: parse time of the chosen headers in every TU (nested headers counted once) "
                   "x (1 - load cost %.2f) minus building the PCH once. The headers to put into the PCH are "
                   "the `roots`: chosen headers not nested into other chosen ones. Any change of a chosen header "
                   "rebuilds all users of the PCH." % args.pch_load_cost)
        out.append("")
        out.append("C++ TUs with traces: %d, distinct compiler commands: %d." % (len(all_cxx), len(flag_groups)))
        out.append("")

        def without_own(target, selected):
            if args.pch_include_own:
                return selected
            own = target[2:].split(":")[0] + "/"
            return set(h for h in selected if not headers.names[h].startswith(own))

        def chosen(group, coverage):
            presence = collections.Counter()
            for tu in group:
                presence.update(set(tu.h_ids))
            return set(h for h, c in presence.items() if c >= coverage * len(group))

        pch_rows = []
        unit_roots = {}
        for (target, flags), utus in units.items():
            if len(utus) < args.pch_min_tus:
                continue
            selected = without_own(target, chosen(utus, args.pch_coverage))
            if not selected:
                continue
            saving, parse, roots = pch_estimate(utus, selected, args.pch_load_cost)
            if saving <= 0:
                continue
            clang = sum(tu.totals.get("ExecuteCompiler", 0) for tu in utus)
            key = (target, flags)
            pch_rows.append((saving, key, len(utus), parse, clang, len(selected)))
            unit_roots[key] = sorted(roots.items(), key=lambda kv: -kv[1][0])

        pch_rows.sort(key=lambda r: -r[0])
        out.append("### Per target")
        out.append("")
        out.append("`flags` is the flag group number (see below). A target with several groups (`groups` > 1) "
                   "compiles its sources with different flags: a gn `precompiled_header` serves only one of them, "
                   "the target has to be split first.")
        out.append("")

        def unit_row(r, fmt):
            saving, (target, flags), n, parse, clang, k = r
            return [target, flag_group_no[flags], flags_per_target[target], n, fmt(saving),
                    "%.0f%%" % (100.0 * saving / max(clang, 1)), fmt(parse), k]
        md_table(out, ["target", "flags", "groups", "C++ TUs", "est. saving", "share of clang time",
                       "chosen headers parse", "headers", "top roots"],
                 [unit_row(r, fmt_h) + [", ".join("`%s`" % headers.names[h] for h, _ in unit_roots[r[1]][:5])]
                  for r in pch_rows[:top]])
        write_csv(report_dir, "pch_targets.csv",
                  ["target", "flag_group", "groups_in_target", "cxx_tus", "est_saving_s", "share_of_clang",
                   "chosen_parse_s", "chosen_headers"],
                  [unit_row(r, fmt_s) for r in pch_rows])
        write_csv(report_dir, "pch_target_roots.csv", ["target", "flag_group", "header", "parse_s", "tus"],
                  [(r[1][0], flag_group_no[r[1][1]], headers.names[h], fmt_s(v[0]), v[1])
                   for r in pch_rows for h, v in unit_roots[r[1]]])

        # common PCH: the same content for everybody, but a PCH binary is valid only within a flag group
        presence = collections.Counter()
        for tu in all_cxx:
            presence.update(set(tu.h_ids))
        selected = set(h for h, c in presence.items() if c >= args.pch_global_coverage * len(all_cxx))

        per_unit_saving = 0
        for (target, flags), utus in units.items():
            if len(utus) >= args.pch_min_tus:
                saving, _, _ = pch_estimate(utus, selected, args.pch_load_cost)
                per_unit_saving += max(saving, 0)

        shared_saving = 0
        total_roots = collections.defaultdict(lambda: [0, 0])
        group_rows = []
        base_tokens = None
        for flags, gtus in sorted(flag_groups.items(), key=lambda kv: -len(kv[1])):
            tokens = flags.split()
            if base_tokens is None:
                base_tokens = tokens
            saving, parse, roots = pch_estimate(gtus, selected, args.pch_load_cost)
            if saving > 0:
                shared_saving += saving
                for h, v in roots.items():
                    total_roots[h][0] += v[0]
                    total_roots[h][1] += v[1]
            base = set(base_tokens)
            own = set(tokens)
            diff = ["+" + t for t in tokens if t not in base] + ["-" + t for t in base_tokens if t not in own]
            if not diff and tokens != base_tokens:
                # e.g. include directories in another order: still a different PCH
                diff = ["(same flags, different order)"]
            group_targets = sorted(set(tu.target for tu in gtus))
            group_rows.append((flag_group_no[flags], len(gtus), len(group_targets), max(saving, 0), diff,
                               group_targets, flags))

        rows = sorted(total_roots.items(), key=lambda kv: -kv[1][0])
        out.append("### Common PCH")
        out.append("")
        out.append("Headers included by at least %.0f%% of all %d C++ TUs (%d headers). Estimated saving:"
                   % (100 * args.pch_global_coverage, len(all_cxx), len(selected)))
        out.append("")
        out.append("* %s - gn `precompiled_header` with this content in every target (one PCH build per target "
                   "and flag group with at least %d TUs);" % (fmt_h(per_unit_saving), args.pch_min_tus))
        out.append("* %s - one PCH build per flag group shared by all its targets (needs a custom action in gn)."
                   % fmt_h(shared_saving))
        out.append("")
        out.append("The roots below are the headers to list in the common PCH.")
        out.append("")
        md_table(out, ["root header", "parse", "TUs"],
                 [(headers.names[h], fmt_h(v[0]), v[1]) for h, v in rows[:top]])
        write_csv(report_dir, "pch_common_roots.csv", ["header", "parse_s", "tus"],
                  [(headers.names[h], fmt_s(v[0]), v[1]) for h, v in rows])
        write_csv(report_dir, "pch_common_headers.csv", ["header", "tus"],
                  sorted(((headers.names[h], presence[h]) for h in selected), key=lambda r: -r[1]))

        out.append("### Flag groups")
        out.append("")
        out.append("Difference of each group's command from the largest group #1: what prevents sharing a PCH "
                   "(`+` extra, `-` missing tokens). Full commands are in pch_flag_groups.csv.")
        out.append("")
        md_table(out, ["group", "C++ TUs", "targets", "common PCH saving", "difference from #1", "targets (sample)"],
                 [(n, t, k, fmt_h(s), short(" ".join("`%s`" % x for x in d[:12]) + (" ..." if len(d) > 12 else ""),
                                            400) or "-",
                   ", ".join(g[:3]) + (", ..." if len(g) > 3 else ""))
                  for n, t, k, s, d, g, _ in group_rows[:top]])
        write_csv(report_dir, "pch_flag_groups.csv",
                  ["group", "cxx_tus", "targets", "common_pch_saving_s", "difference", "target_list", "command"],
                  [(n, t, k, fmt_s(s), " ".join(d), " ".join(g), f) for n, t, k, s, d, g, f in group_rows])

    report = os.path.join(report_dir, "report.md")
    with open(report, "w") as f:
        f.write("\n".join(out) + "\n")
    log("report: %s" % report)


if __name__ == "__main__":
    main()
