#!/usr/bin/env python3
"""Loops `ninja -C <out> -k 0 <targets>`, fixing two kinds of failures on the fly
by uncommenting an edge that is already in the BUILD.gn (nothing else):

 1. "fatal error: '<x>' file not found" while compiling an object of target A:
    the first header of the include chain not of A's modules belongs to
    module B; A's commented edge to a target of B gets uncommented.
 2. "undefined symbol: S" referenced by an object of target A: A's commented
    edge to the target whose built .so/objects define S gets uncommented.

Never touches the edges noted `# link slot` / `# no target`, nor a testonly
edge of a target that is not testonly. An edge gn gen rejects (a cycle, ...)
is commented back and listed in the blocked file, so it is not tried again.
A run whose build narrows down to a tail (fewer than half of the CPUs busy for
20 s) after a fix is interrupted, and the next run starts.

    autofix.py --out <build dir> --log-dir <dir> [--root <source root>]
               [--blocked <file>] [-j N] [--summary <json>] <target>...

Stops after a run that fixed nothing, built everything, or when gn
regeneration fails for a reason that is none of its edges. Logs into
--log-dir: run<N>.log (ninja output), fixes.log, unresolved<N>.log,
summary.log; --summary gets the result as JSON. Exit code: that of the last
ninja run.
"""

import argparse
import glob
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import yamake2gn as g  # noqa: E402  (target_blocks, Resolver)

# set by main()
ROOT = None
OUT = None
LOG_DIR = None
BLOCKED = None
NM = None

STATUS_RE = re.compile(r"^\[\d+/\d+\] ")
INCLUDED_RE = re.compile(r"^In file included from (.+?):\d+:$")
NOTFOUND_RE = re.compile(r"^(.+?):\d+:\d+: fatal error: '([^']+)' file not found")
UNDEF_RE = re.compile(r"^ld\.lld[-\d]*: error: undefined symbol: (.+)$")
REFOBJ_RE = re.compile(r"^>>>\s+(obj/\S+?\.o)\b")
ENTRY_RE = re.compile(r'^(\s*)#\s*("([^"]+)",.*)$')
LIST_RE = re.compile(r"^\s*(\w+)\s*=\s*\[")


def log(path, line):
    with open(os.path.join(LOG_DIR, path), "a") as f:
        f.write(line + "\n")


def find_nm():
    """llvm-nm of the system, or else the one next to the clang the build uses."""
    for name in ("llvm-nm-18", "llvm-nm"):
        path = shutil.which(name)
        if path:
            return path
    r = subprocess.run([sys.executable, os.path.join(ROOT, "build/gn/scripts/find_ya_clang.py"), ROOT],
                       stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, universal_newlines=True)
    path = os.path.join(r.stdout.strip(), "llvm-nm")
    if r.stdout.strip() and os.path.exists(path):
        return path
    sys.exit("autofix.py: no llvm-nm found")


# ---- source tree ------------------------------------------------------------

def rel_path(p):
    """A path of the compiler output -> source-root-relative (gen/ stripped),
    or None (system header)."""
    a = os.path.normpath(os.path.join(OUT, p))
    for base in (os.path.join(OUT, "gen") + "/", ROOT + "/"):
        if a.startswith(base):
            return a[len(base):]
    return None


_resolver = []


def resolver():
    """The generator's Resolver: which module owns a file (aliases, absorbed
    modules, include/ydb-cpp-sdk, contrib include roots)."""
    if not _resolver:
        mods = {d: g.parse_yamake(os.path.join(ROOT, d, "ya.make"), d, ROOT)
                for d in g.find_yamakes(ROOT, ROOT)}
        res = g.Resolver(ROOT, mods, g.ExternalTargets(ROOT))
        g.merge_aliases(mods, res)
        g.link_foreign_sources(mods, res)
        _resolver.append(res)
    return _resolver[0]


def resolve_include(inc, file_dir):
    """The source-root-relative path of a missing #include, or None."""
    rel = resolver().resolve(inc, False, file_dir)
    if rel is None and not inc.startswith((".", "/")) and os.path.isdir(
            os.path.join(ROOT, os.path.dirname(inc))):
        rel = os.path.normpath(inc)
    return rel


_mod_cache = {}


def nearest_module(rel):
    owner = resolver().nearest_module(rel)
    if owner is not None:
        return owner
    d = os.path.dirname(rel)
    if d in _mod_cache:
        return _mod_cache[d]
    x = d
    while x and not os.path.exists(os.path.join(ROOT, x, "ya.make")):
        x = os.path.dirname(x)
    _mod_cache[d] = x
    return x


def norm_label(label, host):
    label = label.split("(")[0]
    if label.startswith(":"):
        return host, label[1:]
    d, _, name = label[2:].partition(":")
    return d, name or os.path.basename(d)


def read(path):
    try:
        with open(path) as f:
            return f.read()
    except OSError:
        return None


def block(label):
    """(text, body offset, body) of the target's block in its BUILD.gn, or None."""
    d, name = label
    text = read(os.path.join(ROOT, d, "BUILD.gn"))
    if text is None:
        return None
    blocks = {n: (body, pos) for _, n, body, pos in g.target_blocks(text)}
    while name not in blocks and "_" in name:   # template helper targets
        name = name.rsplit("_", 1)[0]
    if name not in blocks:
        return None
    body, pos = blocks[name]
    return text, pos, body, name


def sources(label):
    b = block(label)
    if b is None:
        return []
    m = re.search(r"(?<![\w])sources\s*=\s*\[(.*?)\]", b[2], re.S)
    if not m:
        return []
    out = []
    for s in re.findall(r'"([^"]+)"', m.group(1)):
        out.append(s[2:] if s.startswith("//") else os.path.normpath(os.path.join(label[0], s)))
    return out


def modules(label):
    srcs = sources(label)
    if srcs:
        return {nearest_module(s) for s in srcs}
    d, name = label
    if name != os.path.basename(d) and os.path.exists(os.path.join(ROOT, d, name, "ya.make")):
        return {os.path.join(d, name)}
    return {d}


def commented_edges(label):
    """[(label as written, (dir, name), note)] of the target's commented deps."""
    b = block(label)
    if b is None:
        return []
    out, cur = [], None
    for line in b[2].splitlines():
        m = LIST_RE.match(line)
        if m:
            cur = m.group(1) if "]" not in line else None
            continue
        if line.strip().startswith("]"):
            cur = None
            continue
        if cur in ("deps", "public_deps"):
            e = ENTRY_RE.match(line)
            if e:
                note = e.group(2).split("#", 1)[1].strip() if "#" in e.group(2) else ""
                out.append((e.group(3), norm_label(e.group(3), label[0]), note))
    return out


def uncomment(label, written):
    """Uncomment `written` in the deps of the target; True if done."""
    b = block(label)
    if b is None:
        return False
    text, pos, body, _ = b
    lines = body.split("\n")
    for i, line in enumerate(lines):
        e = ENTRY_RE.match(line)
        if e and e.group(3) == written:
            lines[i] = e.group(1) + e.group(2)
            new = "\n".join(lines)
            with open(os.path.join(ROOT, label[0], "BUILD.gn"), "w") as f:
                f.write(text[:pos] + new + text[pos + len(body):])
            return True
    return False


def recomment(label, written):
    """Comment `written` out again in the deps of the target; True if done."""
    b = block(label)
    if b is None:
        return False
    text, pos, body, _ = b
    lines = body.split("\n")
    for i, line in enumerate(lines):
        m = re.match(r'^(\s*)("([^"]+)",.*)$', line)
        if m and m.group(3) == written:
            lines[i] = m.group(1) + "# " + m.group(2)
            new = "\n".join(lines)
            with open(os.path.join(ROOT, label[0], "BUILD.gn"), "w") as f:
                f.write(text[:pos] + new + text[pos + len(body):])
            return True
    return False


def testonly(label):
    b = block(label)
    return b is not None and re.search(r"^\s*testonly\s*=\s*true", b[2], re.M) is not None


def blocked():
    """{(A, written)}: uncommenting them broke gn gen."""
    out = set()
    for line in (read(BLOCKED) or "").splitlines():
        d, name, w = line.split()
        out.add(((d, name), w))
    return out


def our_fixes():
    """[(A, written)] uncommented by this process (fixes.log), latest last."""
    out = []
    for line in (read(os.path.join(LOG_DIR, "fixes.log")) or "").splitlines():
        m = re.match(r"^run\d+ \d //(\S+?):(\S+)  uncommented (\S+)  ", line)
        if m:
            out.append(((m.group(1), m.group(2)), m.group(3)))
    return out


def undo_regen_failure(text, n):
    """Re-comment and block our edges gn gen rejects (a test-only dependency,
    a cycle); True if anything was undone."""
    labels = set()
    m = re.search(r"\n(//\S+)\nwhich is NOT marked testonly can't depend on\n(//\S+)\n", text)
    if m:
        labels = {(norm_label(m.group(1), ""), norm_label(m.group(2), ""))}
    cyc = re.search(r"Dependency cycle:\n((?:\s+//\S+.*\n)+)", text)
    if cyc:
        chain = [norm_label(x, "") for x in re.findall(r"(//[^\s(]+)", cyc.group(1))]
        # helper targets of a library (serialize_enums' <t>_enum_objs, ...) stand for it
        chain = [(d, re.sub(r"_(enum_objs|enum_foreign|enum|proto|codegen|link_slot_check)$", "", n)) for d, n in chain]
        labels |= set(zip(chain, chain[1:]))
    # a target of ours newly reached has a dep that does not exist
    broken = {norm_label(x, "") for x in re.findall(
        r"Unresolved dependencies\.\n(//[^\s(]+)", text)}
    undone = False
    for a, w in our_fixes():
        if ((a, norm_label(w, a[0])) in labels or norm_label(w, a[0]) in broken) \
                and (a, w) not in blocked():
            if recomment(a, w):
                with open(BLOCKED, "a") as f:
                    f.write("%s %s %s\n" % (a[0], a[1], w))
                log("fixes.log", "run%d - //%s:%s  re-commented %s  (gn gen rejects it)" % (n, a[0], a[1], w))
                undone = True
    return undone


SKIP_NOTES = ("link slot", "no target")


# ---- build dir --------------------------------------------------------------

class NinjaIndex:
    """obj -> (dir, target) and (dir, target) -> its .so / objects, from the
    per-target .ninja files gn writes."""

    def __init__(self):
        self.obj, self.outs = {}, {}
        for f in glob.glob(os.path.join(OUT, "obj", "**", "*.ninja"), recursive=True):
            rel = os.path.relpath(f, os.path.join(OUT, "obj"))
            label = (os.path.dirname(rel), os.path.basename(rel)[:-len(".ninja")])
            outs = []
            with open(f) as fh:
                for line in fh:
                    if line.startswith("build "):
                        targets = line[6:].split(":", 1)[0].split()
                        for t in targets:
                            t = os.path.normpath(t)
                            if t.endswith(".o"):
                                self.obj[t] = label
                            outs.append(t)
            self.outs[label] = outs

    def owner(self, obj):
        return self.obj.get(os.path.normpath(obj))

    def binaries(self, label):
        """The built .so (or else objects) of a target and its helpers."""
        d, name = label
        for helpers in (False, True):
            outs = [o for (dd, n), os_ in self.outs.items()
                    if dd == d and (n.startswith(name + "_") if helpers else n == name) for o in os_]
            so = [o for o in outs if o.endswith(".so") and os.path.exists(os.path.join(OUT, o))]
            if so:
                return so, True
            objs = [o for o in outs if o.endswith(".o") and os.path.exists(os.path.join(OUT, o))]
            if objs:
                return objs, False
        return [], False


_nm_cache = {}


def defines(path, dynamic):
    full = os.path.join(OUT, path)
    key = (full, os.path.getmtime(full))
    if key not in _nm_cache:
        args = [NM, "-C", "--defined-only"] + (["-D"] if dynamic else []) + [full]
        r = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, universal_newlines=True)
        syms = set()
        for line in r.stdout.splitlines():
            parts = line.split(None, 2)
            if len(parts) == 3:
                syms.add(parts[2].split("@")[0])
        _nm_cache[key] = syms
    return _nm_cache[key]


# ---- the two fixes ----------------------------------------------------------

class Run:
    def __init__(self, n):
        self.n, self.index, self.fixed, self.unresolved = n, None, [], []
        self.done = set()   # (A, written) uncommented in this run

    def idx(self):
        if self.index is None:
            self.index = NinjaIndex()
        return self.index

    def fix(self, kind, a, written, why):
        if (a, written) in self.done:
            return
        if (a, written) in blocked():
            return self.miss(kind, "//%s:%s: %s: %s -- blocked (gn gen rejected it)" % (a[0], a[1], why, written))
        if testonly(norm_label(written, a[0])) and not testonly(a):
            return self.miss(kind, "//%s:%s: %s: %s is testonly, the target is not" % (a[0], a[1], why, written))
        if uncomment(a, written):
            self.done.add((a, written))
            self.fixed.append((kind, a, written, why))
            log("fixes.log", "run%d %s //%s:%s  uncommented %s  (%s)" % (self.n, kind, a[0], a[1], written, why))

    def miss(self, kind, what):
        if (kind, what) not in self.unresolved:
            self.unresolved.append((kind, what))
            log("unresolved%d.log" % self.n, "%d %s" % (kind, what))

    def header(self, obj, chain, missing):
        a = self.idx().owner(obj)
        if a is None:
            return self.miss(1, "%s: no target for the object" % obj)
        own = modules(a)
        files = [rel_path(p) for p in chain]
        files = [f for f in files[1:] if f]   # headers, the TU's source dropped
        last = rel_path(chain[-1])
        inc = resolve_include(missing, os.path.dirname(last) if last else "")
        if inc:
            files.append(inc)
        hdr = next((f for f in files if nearest_module(f) not in own), None)
        if hdr is None:
            return self.miss(1, "//%s:%s: '%s' -- no header of another module in the chain" % (a[0], a[1], missing))
        b = nearest_module(hdr)
        if any(b in modules(norm_label(w, a[0])) for aa, w in self.done if aa == a):
            return   # uncommented earlier in this run
        cands = [(w, l, note) for w, l, note in commented_edges(a) if b in modules(l)]
        if len(cands) > 1:
            exact = [c for c in cands if hdr in sources(c[1])]
            cands = exact or cands
        cands = [c for c in cands if not any(c[2].startswith(s) for s in SKIP_NOTES)]
        if len(cands) != 1:
            return self.miss(1, "//%s:%s: %s (module %s): %s" % (
                a[0], a[1], hdr, b, "no commented edge" if not cands else
                "ambiguous " + " ".join(c[0] for c in cands)))
        self.fix(1, a, cands[0][0], hdr)

    def symbol(self, sym, obj):
        a = self.idx().owner(obj)
        if a is None:
            return self.miss(2, "%s: %s: no target for the object" % (sym, obj))
        for aa, w in self.done:
            if aa == a:
                files, dynamic = self.idx().binaries(norm_label(w, a[0]))
                if any(sym in defines(f, dynamic) for f in files):
                    return   # uncommented earlier in this run
        hits, unbuilt = [], []
        for w, l, note in commented_edges(a):
            if any(note.startswith(s) for s in SKIP_NOTES):
                continue
            files, dynamic = self.idx().binaries(l)
            if not files:
                unbuilt.append(w)
            elif any(sym in defines(f, dynamic) for f in files):
                hits.append(w)
        if len(hits) != 1:
            return self.miss(2, "//%s:%s: %s: %s" % (
                a[0], a[1], sym, ("ambiguous " + " ".join(hits)) if hits else
                "no commented edge defines it" + (" (unbuilt: %s)" % " ".join(unbuilt) if unbuilt else "")))
        self.fix(2, a, hits[0], sym)

    def failure(self, outputs, lines):
        chain, undef = [], None
        for line in lines:
            m = INCLUDED_RE.match(line)
            if m:
                chain.append(m.group(1))
                continue
            m = NOTFOUND_RE.match(line)
            if m:
                obj = next((o for o in outputs if o.endswith(".o")), None)
                if obj:
                    self.header(obj, chain + [m.group(1)], m.group(2))
                chain = []
                continue
            if not line.startswith("In file included"):
                chain = []
            m = UNDEF_RE.match(line)
            if m:
                undef = m.group(1)
                continue
            m = REFOBJ_RE.match(line)
            if m and undef:
                self.symbol(undef, m.group(1))
                undef = None   # one referencing object is enough


# ninja's jobs below half of the cores for this long, with a fix done in the
# run: the rest of the run is a narrow tail -- stop it, start the next one
NARROW_JOBS = (os.cpu_count() or 2) // 2
NARROW_SECONDS = 20


def jobs(pid):
    """The number of ninja's child processes (its running jobs)."""
    count = 0
    for d in os.listdir("/proc"):
        if d.isdigit():
            try:
                with open("/proc/%s/stat" % d) as f:
                    if int(f.read().rsplit(")", 1)[1].split()[1]) == pid:
                        count += 1
            except (OSError, IndexError, ValueError):
                pass
    return count


def watch_narrow(p, r, state):
    """Interrupt ninja once the run narrows down (see NARROW_JOBS)."""
    since = None
    while p.poll() is None:
        time.sleep(2)
        if not state["building"] or not r.fixed:
            since = None
            continue
        if jobs(p.pid) < NARROW_JOBS:
            since = since or time.time()
            if time.time() - since >= NARROW_SECONDS:
                state["narrow"] = True
                p.send_signal(signal.SIGINT)
                return
        else:
            since = None


def run(n, targets, ninja_jobs):
    r = Run(n)
    log_path = os.path.join(LOG_DIR, "run%d.log" % n)
    out = open(log_path, "w", buffering=1)
    cmd = ["ninja", "-C", OUT, "-k", "0"] + (["-j", str(ninja_jobs)] if ninja_jobs else []) + targets
    env = dict(os.environ, NINJA_STATUS="[%f/%t] ")   # STATUS_RE
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env,
                         universal_newlines=True, errors="replace")
    state = {"building": False, "narrow": False}
    threading.Thread(target=watch_narrow, args=(p, r, state), daemon=True).start()
    cur, regen_failed, last_status = None, False, ""
    for line in p.stdout:
        out.write(line)
        line = line.rstrip("\n")
        if line.startswith("FAILED: ") or STATUS_RE.match(line) or line.startswith("ninja: "):
            if cur:
                r.failure(*cur)
                cur = None
            if line.startswith("FAILED: "):
                cur = (line[8:].split(), [])
            if "rebuilding 'build.ninja'" in line or line.startswith("ERROR at "):
                regen_failed = True
            if STATUS_RE.match(line):
                last_status = line
                state["building"] = "Regenerating ninja files" not in line
        elif cur is not None:
            cur[1].append(line)
        elif line.startswith("ERROR at "):
            regen_failed = True
    if cur:
        r.failure(*cur)
    p.wait()
    out.close()
    undone = regen_failed and undo_regen_failure(read(log_path), n)
    with open(log_path) as f:
        failed = sum(1 for l in f if l.startswith("FAILED: "))
    log("summary.log", "run%d rc=%d failed=%d fixed=%d unresolved(1/2)=%d regen_failed=%s%s last=%s %s" % (
        n, p.returncode, failed, len(r.fixed), len(r.unresolved), regen_failed,
        " stopped-narrow" if state["narrow"] else "", last_status[:40], time.strftime("%H:%M:%S")))
    return r, regen_failed, undone, p.returncode, failed


def main():
    global ROOT, OUT, LOG_DIR, BLOCKED, NM
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("targets", nargs="+")
    ap.add_argument("--out", required=True, help="the build dir (gn gen done)")
    ap.add_argument("--root", default=os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))
    ap.add_argument("--log-dir", required=True)
    ap.add_argument("--blocked", help="edges gn gen rejected, kept across invocations (default: <log dir>/blocked.txt)")
    ap.add_argument("-j", "--jobs", type=int, help="ninja -j")
    ap.add_argument("--summary", help="write the result as JSON here")
    args = ap.parse_args()
    ROOT, OUT = os.path.realpath(args.root), os.path.realpath(args.out)
    g._build_dir = g._lsi().build_dir_under(ROOT, OUT)  # a build dir inside the tree is not scanned
    LOG_DIR = os.path.realpath(args.log_dir)
    os.makedirs(LOG_DIR, exist_ok=True)
    BLOCKED = os.path.realpath(args.blocked) if args.blocked else os.path.join(LOG_DIR, "blocked.txt")
    NM = find_nm()

    n, fixed, started = 1, [], time.time()
    while True:
        r, regen_failed, undone, rc, failed = run(n, args.targets, args.jobs)
        fixed += ["run%d %d //%s:%s %s (%s)" % (n, k, a[0], a[1], w, why) for k, a, w, why in r.fixed]
        if rc == 0 or (regen_failed and not undone) or not (r.fixed or undone):
            break
        n += 1
    if args.summary:
        with open(args.summary, "w") as f:
            json.dump({"rc": rc, "runs": n, "seconds": round(time.time() - started, 1),
                       "regen_failed": regen_failed and not undone, "failed": failed, "fixed": fixed,
                       "unresolved": ["%d %s" % u for u in r.unresolved]}, f, indent=1)
    return rc


if __name__ == "__main__":
    sys.exit(main())
