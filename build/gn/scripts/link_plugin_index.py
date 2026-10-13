"""The plugins every executable links (//build/gn/link_slots.gni), gathered
from the BUILD.gn files of the tree.

A library names the targets built on top of it that it needs in the process
at run time -- implementations it finds in a registry (a static registration,
SRCS(GLOBAL ...) of ya.make), which reference the library, not the other way
round:

    library("engines") {
      plugins = [ "//ydb/core/tx/columnshard/engines/storage" ]
    }

A dependency on them would be a cycle. An executable declared with
linked_executable() or a template of //build/gn/testing.gni depends on the
plugins of every target it reaches instead: through deps and public_deps, the
providers of its link_select and the plugins themselves, until nothing new
comes up.

The files are parsed by gn itself, as in link_slot_index.py: a list must be
written literally in the target's block; a label with $-expansions, a target
created by a template with a computed name and the deps a template adds on
its own (the default ones) are not seen.

Usage: link_plugin_index.py <source root> <gn binary>
Prints a gn scope (for exec_script(..., "scope")):

    executables = [ { label = "//dir:name"  plugins = [ ... ] }, ... ]
"""

import os
import re
import sys

import link_slot_index as lsi

KEY = "plugins"
EXECUTABLE = "linked_executable"
TESTING = "build/gn/testing.gni"   # the templates of tests, executables too
DEP_KEYS = ("deps", "public_deps")


def all_build_files(root):
    out = []
    for d, dirs, files in os.walk(root):
        dirs[:] = sorted(x for x in dirs if not x.startswith("."))
        if lsi.BUILD_FILE in files:
            out.append(os.path.relpath(os.path.join(d, lsi.BUILD_FILE), root))
    return out


def executables(root):
    with open(os.path.join(root, TESTING), encoding="utf-8") as f:
        return {EXECUTABLE} | set(re.findall(r'^template\("([^"]+)"\)', f.read(), re.M))


def mentions_plugins(root, files):
    key = re.compile(rb"^\s*" + KEY.encode() + rb"\s*\+?=", re.M)
    for f in files:
        with open(os.path.join(root, f), "rb") as fh:
            if key.search(fh.read()):
                return True
    return False


def _calls(node):
    """(function, name, block) of every call with a literal name and a block."""
    for st in node.get("child", []):
        t = st.get("type")
        if t == "FUNCTION":
            kids = st.get("child", [])
            if len(kids) == 2 and kids[0].get("type") == "LIST" and kids[1].get("type") == "BLOCK":
                args = kids[0].get("child", [])
                name = lsi._string(args[0]) if len(args) == 1 else None
                if name is not None:
                    yield st.get("value"), name, kids[1]
        elif t == "CONDITION":
            for sub in st.get("child", [])[1:]:
                if sub.get("type") in ("BLOCK", "CONDITION"):
                    yield from _calls(sub if sub["type"] == "BLOCK" else {"child": [sub]})


def _strings(block, key):
    """Plain strings of `key`, the rest (computed values) skipped."""
    out = []
    for st in block.get("child", []):
        t = st.get("type")
        if t == "BINARY" and st.get("value") in ("=", "+="):
            lhs, rhs = st["child"]
            if lhs.get("type") == "IDENTIFIER" and lhs.get("value") == key:
                items = rhs.get("child", []) if rhs.get("type") == "LIST" else [rhs]
                out += [s for s in map(lsi._string, items) if s is not None]
        elif t == "CONDITION":
            for sub in st.get("child", [])[1:]:
                if sub.get("type") == "BLOCK":
                    out += _strings(sub, key)
                elif sub.get("type") == "CONDITION":
                    out += _strings({"child": [sub]}, key)
    return out


def resolve(file, label):
    """A label as written in `file` -> //dir:name, or None (toolchain, $)."""
    if "(" in label:
        label = label[:label.index("(")]
    here = os.path.dirname(file).replace(os.sep, "/")
    if label.startswith(":"):
        d, name = here, label[1:]
    else:
        d, _, name = label.partition(":")
        d = d[2:] if d.startswith("//") else os.path.normpath(os.path.join(here, d)).replace(os.sep, "/")
        name = name or os.path.basename(d)
    return "//%s:%s" % (d, name)


def full(label):
    """//dir -> //dir:dir, the form resolve() gives."""
    d, _, name = label[2:].partition(":")
    return "//%s:%s" % (d, name or os.path.basename(d))


def short(label):
    d, _, name = label[2:].partition(":")
    return "//%s" % d if name == os.path.basename(d) else label


def index(root, gn):
    files = all_build_files(root)
    if not mentions_plugins(root, files):
        return {}
    exes = executables(root)
    deps, plugins, selects = {}, {}, {}
    for file, tree in lsi.dump_trees(root, gn, files):
        for func, name, block in _calls(tree):
            me = full(lsi.label(file, name))
            deps[me] = [resolve(file, l) for k in DEP_KEYS for l in _strings(block, k)]
            p = [resolve(file, l) for l in _strings(block, KEY)]
            if p:
                plugins[me] = p
            if func in exes:
                selects[me] = _strings(block, "link_select")
    providers = lsi.index(root, gn)[0]

    out = {}
    for exe, select in selects.items():
        queue = list(deps.get(exe, []))
        for entry in select:
            slot, _, value = entry.partition("=")
            queue += [full(l) for l in providers.get((slot, value), [])]
        seen, found = set(), []
        while queue:
            t = queue.pop()
            if t in seen:
                continue
            seen.add(t)
            queue += deps.get(t, [])
            for p in plugins.get(t, []):
                if p not in found:
                    found.append(p)
                queue.append(p)
        if found:
            out[exe] = sorted(short(p) for p in found)
    return out


def main():
    root, gn = sys.argv[1], sys.argv[2]
    print("executables = [")
    for exe, found in sorted(index(root, gn).items()):
        print("  {\n    label = %s\n    plugins = [ %s ]\n  }," % (
            lsi._gn_string(exe), ", ".join(map(lsi._gn_string, found))))
    print("]")


if __name__ == "__main__":
    main()
