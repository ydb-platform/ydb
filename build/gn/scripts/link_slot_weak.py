"""The weak references to a link slot (//build/gn/link_slots.gni), taken from
the slot's interface headers without touching them.

The functions a header declares are read from clang's AST (-ast-dump=json:
declarations with their mangled names), less the inline ones and those the
interface target itself defines (its own sources, read the same way). The
rest is what a provider implements; the output, force-included into the
consumers, marks it weak at the assembler level:

    #if !defined(GN_SLOT_PROVIDER_<slot>) && !defined(GN_SLOT_INTERFACE_<slot>)
    __asm__(".weak <mangled name>");
    #endif

so a caller gets a weak undefined reference, as with __attribute__((weak)) on
the declaration, while a provider keeps its definitions strong, and so does
the interface target itself (the list comes to it too, as a public config).

A class with an out-of-line virtual function (its key function) has its
vtable and typeinfo emitted by the provider too: a caller that constructs it
or derives from it refers to them, so they are made weak as well.

The compiler flags are those of a translation unit of the interface target
(or any other) in compile_commands.json.

Usage: link_slot_weak.py --slot S --compile-commands F --output O
                         --header H... [--source C...] [--flags-from C]
                         [--depfile D] [--root R]

In the build (link_slot_weak_refs in //build/gn/link_slots.gni) only the
headers are given: the functions the interface target defines itself stay in
the list, harmless (weak references its dependents resolve all the same) and
parsing its sources would cost as much as compiling them.
"""

import argparse
import concurrent.futures
import json
import os
import re
import shlex
import subprocess
import sys

FUNCTION_KINDS = ("FunctionDecl", "CXXMethodDecl", "CXXConstructorDecl",
                  "CXXDestructorDecl", "CXXConversionDecl")

# flags of a compile command that name its input/output or a precompiled header
_DROP_WITH_ARG = {"-o", "-MF", "-MT", "-MQ", "-include-pch"}
_DROP = {"-c", "-MD", "-MMD", "-fsyntax-only"}


def compile_argv(entries, source, root):
    """argv of the compile command of `source` (or of any C++ TU), without its
    input, output, dependency file and precompiled header."""
    want = os.path.realpath(os.path.join(root, source)) if source else None
    chosen = same_dir = any_cpp = None
    for e in entries:
        f = os.path.realpath(os.path.join(e["directory"], e["file"]))
        if want and f == want:
            chosen = e
            break
        if not f.endswith((".cpp", ".cc")):
            continue
        if same_dir is None and want and os.path.dirname(f) == os.path.dirname(want):
            same_dir = e
        if any_cpp is None:
            any_cpp = e
    chosen = chosen or same_dir or any_cpp
    if chosen is None:
        sys.exit("link_slot_weak: no compile command to borrow flags from")
    argv = shlex.split(chosen["command"]) if "command" in chosen else list(chosen["arguments"])
    out, skip, skip3 = [], False, 0
    for i, a in enumerate(argv):
        if skip:
            skip = False
            continue
        if skip3:
            skip3 -= 1
            continue
        if a in _DROP_WITH_ARG:
            skip = True
            continue
        if a in _DROP or a == chosen["file"]:
            continue
        # force-included files: a precompiled header, or another slot's weak
        # references (-Xclang -include -Xclang <file>) -- the headers parse
        # without them, and they may not be built yet
        if a == "-include":
            skip = True
            continue
        if a == "-Xclang" and argv[i + 1:i + 3] == ["-include", "-Xclang"]:
            skip3 = 3
            continue
        out.append(a)
    return chosen["directory"], out


def ast(directory, argv, filters, text=None, path=None, deps=()):
    """The JSON AST dumps of the declarations under the names `filters`
    (all of them if none) of a translation unit: `text`, or the file `path`.
    The whole dump of a real TU is gigabytes; one namespace, tens of MB.
    One clang per filter, run in parallel; `deps` (dependency file flags)
    go to the first one only, the file is the same for all."""
    src = [path] if path else ["-x", "c++", "-"]

    def run(f):
        flt = ["-Xclang", "-ast-dump-filter=" + f] if f else []
        dep = list(deps) if f == (filters or [None])[0] else []
        proc = subprocess.run(argv + dep + ["-fsyntax-only", "-Wno-unused-command-line-argument",
                                     "-Xclang", "-ast-dump=json"] + flt + src,
                              cwd=directory, input=text, capture_output=True, text=True)
        if proc.returncode != 0:
            sys.exit("link_slot_weak: clang failed:\n%s" % proc.stderr[-4000:])
        return proc.stdout

    out = []
    with concurrent.futures.ThreadPoolExecutor(len(filters or [None])) as pool:
        for s in pool.map(run, filters or [None]):
            decoder, pos = json.JSONDecoder(), 0
            while True:
                pos = s.find("{", pos)   # a filtered dump prefixes each tree with "Dumping <name>:"
                if pos < 0:
                    break
                tree, pos = decoder.raw_decode(s, pos)
                out.append(tree)
    return out


_NS_RE = re.compile(r"\bnamespace\s+([A-Za-z_]\w*)")
_STRIP_RE = re.compile(r'//[^\n]*|/\*.*?\*/|"(?:\\.|[^"\\\n])*"|\'(?:\\.|[^\'\\\n])*\'', re.S)


def dump_filters(headers):
    """What the headers declare at file scope -- the namespaces they open and
    the functions outside of any (extern "C" ones): the filters of the dumps.
    A declaration missed here stays a strong reference, and then fails the
    consumer's link loudly."""
    out = set()
    for h in headers:
        with open(h, encoding="utf-8", errors="ignore") as f:
            code = _STRIP_RE.sub(" ", f.read())
        code = re.sub(r"^[ \t]*#.*(?:\\\n.*)*$", " ", code, flags=re.M)   # preprocessor lines
        depth, opened = 0, []
        for m in re.finditer(r"\bextern\s*\{|[{}]|\bnamespace\s+[A-Za-z_]\w*|\b([A-Za-z_]\w*)\s*\(", code):
            tok = m.group(0)
            if tok.startswith("extern"):
                opened.append(False)        # extern "C" { ... }: still file scope
            elif tok == "{":
                opened.append(True)
                depth += 1
            elif tok == "}":
                if opened and opened.pop():
                    depth -= 1
            elif depth == 0:
                if tok.startswith("namespace"):
                    out.add(_NS_RE.match(tok).group(1))
                elif m.group(1) not in _NOT_FUNCTIONS:
                    out.add(m.group(1))
    return sorted(out)


# words followed by "(" at file scope that name no function
_NOT_FUNCTIONS = {"deprecated", "nodiscard", "noreturn", "maybe_unused", "alignas", "alignof",
                  "decltype", "sizeof", "noexcept", "static_assert", "__attribute__", "__declspec"}


class Walker:
    """Visits the declarations in dump order, tracking the file each is in:
    the dump names a file only where it changes."""

    def __init__(self, directory):
        self.directory = directory
        self.file = None

    def _track(self, loc):
        if isinstance(loc, dict):
            for k, v in loc.items():   # spellingLoc, then expansionLoc
                if k == "file":
                    self.file = os.path.realpath(os.path.join(self.directory, v))
                elif isinstance(v, dict) and k != "includedFrom":
                    self._track(v)

    def walk(self, node, visit):
        if not isinstance(node, dict):
            return
        self._track(node.get("loc"))
        file = self.file
        self._track(node.get("range"))
        visit(node, file)
        for child in node.get("inner", []):
            self.walk(child, visit)


def _variants(node):
    """The symbols of a function: both constructors/destructors of a class
    (complete and base object; the dump names the complete one)."""
    name = node["mangledName"]
    if node["kind"] == "CXXConstructorDecl" and "C1E" in name:
        return [name, name.replace("C1E", "C2E", 1)]
    if node["kind"] == "CXXDestructorDecl" and "D1E" in name:
        return [name, name.replace("D1E", "D2E", 1)]
    return [name]


def _source_name(m, i):
    """A <source-name> (<length><identifier>) of the mangled name `m` at `i`:
    (the name with its length, the index after it), or None."""
    j = i
    while j < len(m) and m[j].isdigit():
        j += 1
    if j == i:
        return None
    end = j + int(m[i:j])
    return (m[i:end], end) if end <= len(m) else None


def _class_of(mangled):
    """The mangled name of the class whose member function `mangled` is
    (_ZN[cv-qualifiers]<class components><member name>E...), for a class
    named by plain identifiers only -- no templates, substitutions or ABI
    tags; None otherwise."""
    if not mangled.startswith("_ZN"):
        return None
    i = 3
    while i < len(mangled) and mangled[i] in "rVKRO":
        i += 1
    parts = []
    while i < len(mangled) and mangled[i] != "E":
        if mangled[i].isdigit():
            got = _source_name(mangled, i)
            if got is None:
                return None
            name, i = got
            parts.append(name)
        elif mangled[i] in "CD" and i + 1 < len(mangled) and mangled[i + 1].isdigit():
            parts.append(mangled[i:i + 2])      # constructor / destructor
            i += 2
        else:
            return None
    if len(parts) < 2 or i >= len(mangled):
        return None
    cls = parts[:-1]
    return "N%sE" % "".join(cls) if len(cls) > 1 else cls[0]


def _virtual(node):
    return bool(node.get("virtual")) or any(
        c.get("kind") in ("OverrideAttr", "FinalAttr") for c in node.get("inner", []))


def _defines(node):
    return any(c.get("kind") in ("CompoundStmt", "CXXCtorInitializer")
               for c in node.get("inner", []))


def declared(directory, argv, headers, filters, deps=()):
    """Mangled names of the out-of-line functions `headers` declare, and of the
    vtables and typeinfo of the classes whose key function is one of them."""
    wanted = {os.path.realpath(h) for h in headers}
    out = set()

    def visit(node, file):
        if (node.get("kind") in FUNCTION_KINDS and file in wanted and "mangledName" in node
                and not node.get("inline") and not node.get("isImplicit")
                and not node.get("constexpr") and not node.get("pure")
                and not node.get("explicitlyDefaulted") and not node.get("explicitlyDeleted")
                and not _defines(node)):
            out.update(_variants(node))
            if node["kind"] != "FunctionDecl" and _virtual(node):
                cls = _class_of(node["mangledName"])
                if cls:
                    out.update(("_ZTV" + cls, "_ZTI" + cls))

    text = "".join('#include "%s"\n' % os.path.realpath(h) for h in headers)
    for tree in ast(directory, argv, filters, text=text, deps=deps):
        Walker(directory).walk(tree, visit)
    return out


def defined(directory, argv, source, filters):
    """Mangled names of the functions `source` defines."""
    out = set()

    def visit(node, file):
        if node.get("kind") in FUNCTION_KINDS and "mangledName" in node and _defines(node):
            out.update(_variants(node))

    for tree in ast(directory, argv, filters, path=source):
        Walker(directory).walk(tree, visit)
    return out


def render(slot, names):
    lines = ["// Generated by build/gn/scripts/link_slot_weak.py: the weak references",
             "// to the %s link slot, see build/gn/link_slots.gni." % slot,
             "#pragma once",
             "#if !defined(GN_SLOT_PROVIDER_%s) && !defined(GN_SLOT_INTERFACE_%s)" % (slot, slot)]
    lines += ['__asm__(".weak %s");' % n for n in sorted(names)]
    lines += ["#endif", ""]
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--slot", required=True)
    ap.add_argument("--compile-commands", required=True)
    ap.add_argument("--root", default=".")
    ap.add_argument("--header", action="append", default=[])
    ap.add_argument("--source", action="append", default=[])
    ap.add_argument("--flags-from", help="the TU whose flags to use (default: the first --source)")
    ap.add_argument("--output", required=True)
    ap.add_argument("--depfile", help="write the headers the result depends on (for ninja)")
    args = ap.parse_args()

    with open(args.compile_commands, encoding="utf-8") as f:
        entries = json.load(f)
    root = os.path.realpath(args.root)
    headers = [os.path.join(root, h) for h in args.header]
    sources = [os.path.join(root, s) for s in args.source]

    filters = dump_filters(headers)
    directory, argv = compile_argv(entries, args.flags_from or (args.source[0] if args.source else None), root)
    # everything the headers include: a type declared elsewhere is part of
    # the mangled names
    deps = ["-MD", "-MF", args.depfile, "-MT", args.output] if args.depfile else []
    names = declared(directory, argv, headers, filters, deps)

    def own(s):
        d, a = compile_argv(entries, os.path.relpath(s, root), root)
        return defined(d, a, s, filters)

    with concurrent.futures.ThreadPoolExecutor(max(1, min(len(sources), os.cpu_count() or 1))) as pool:
        for d in pool.map(own, sources):
            names -= d

    text = render(args.slot, names)
    try:
        with open(args.output, encoding="utf-8") as f:
            if f.read() == text:
                return      # unchanged: keeps the consumers from recompiling
    except OSError:
        pass
    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        f.write(text)


if __name__ == "__main__":
    main()
