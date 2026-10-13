"""The providers of the link slots (//build/gn/link_slots.gni), gathered from
the BUILD.gn files of the tree -- there is no list of slots to keep by hand.

A target declares what it provides:

    library("llvm16") {
      link_slot_provides = [ "minikql_codegen" ]         # value: "llvm16"
      link_slot_provides = [ "minikql_codegen=llvm16" ]  # the same, spelled out
    }

The value is the target's name unless given. Every target declaring the same
<slot>=<value> is a part of that provider: an executable selecting it depends
on all of them.

The interface of a slot -- the target whose headers declare what the
providers implement -- declares it too:

    library("api") {
      link_slot_interface = "allocator"
      link_slot_headers = [ "malloc.h" ]
    }

A slot with providers and no interface anywhere fails `gn gen`: its
providers' functions would be referenced strongly by every caller, the
executable's selection would mean nothing.

The files are parsed by gn itself (`gn format --dump-tree=json`), not by
regexps: only the syntax is known here, nothing is evaluated -- the list must
be written literally in the target's block (a condition around it is not
looked at), and a target created by a template with a computed name is
skipped.

Usage: link_slot_index.py <source root> <gn binary>
Prints a gn scope (for exec_script(..., "scope")):

    providers = [ { slot = "..."  value = "..."  labels = [ ... ] }, ... ]
"""

import json
import os
import subprocess
import sys

KEY = "link_slot_provides"
INTERFACE_KEY = "link_slot_interface"
BUILD_FILE = "BUILD.gn"


def build_files(root):
    """Repo-relative BUILD.gn files that mention KEY or INTERFACE_KEY at all."""
    out = []
    for d, dirs, files in os.walk(root):
        dirs[:] = sorted(x for x in dirs if not x.startswith("."))
        if BUILD_FILE not in files:
            continue
        path = os.path.join(d, BUILD_FILE)
        with open(path, "rb") as f:
            text = f.read()
            if KEY.encode() in text or INTERFACE_KEY.encode() in text:
                out.append(os.path.relpath(path, root))
    return out


def dump_trees(root, gn, files):
    """[(file, parse tree)], one `gn format` for all of them."""
    if not files:
        return []
    proc = subprocess.run([gn, "format", "--dump-tree=json"] + files, cwd=root,
                          capture_output=True, text=True)
    if proc.returncode != 0:
        sys.exit("link_slot_index: `%s format --dump-tree` failed:\n%s" % (gn, proc.stderr))
    decoder = json.JSONDecoder()
    text, pos, trees = proc.stdout, 0, []
    while True:
        while pos < len(text) and text[pos].isspace():
            pos += 1
        if pos >= len(text):
            break
        tree, pos = decoder.raw_decode(text, pos)
        trees.append(tree)
    if len(trees) != len(files):
        sys.exit("link_slot_index: %d parse trees for %d files" % (len(trees), len(files)))
    return list(zip(files, trees))


def _string(node):
    """The value of a string LITERAL node without $-expansions, else None."""
    if node.get("type") != "LITERAL":
        return None
    v = node.get("value", "")
    if len(v) < 2 or v[0] != '"' or v[-1] != '"' or "$" in v:
        return None
    return v[1:-1]


def _assigned(block, key):
    """Strings assigned or appended to `key` in `block` (a list of them, or
    one), looking into conditions but not into nested calls (another
    target's block)."""
    out = []
    for st in block.get("child", []):
        t = st.get("type")
        if t == "BINARY" and st.get("value") in ("=", "+="):
            lhs, rhs = st["child"]
            if lhs.get("type") == "IDENTIFIER" and lhs.get("value") == key:
                items = rhs.get("child", []) if rhs.get("type") == "LIST" else [rhs]
                for item in items:
                    s = _string(item)
                    if s is None:
                        raise ValueError("%s: not a plain string" % key)
                    out.append(s)
        elif t == "CONDITION":
            for sub in st.get("child", [])[1:]:
                if sub.get("type") == "BLOCK":
                    out += _assigned(sub, key)
                elif sub.get("type") == "CONDITION":   # else if
                    out += _assigned({"child": [sub]}, key)
    return out


def _targets(node):
    """(name, block) of every call with a literal name and a block."""
    for st in node.get("child", []):
        t = st.get("type")
        if t == "FUNCTION":
            kids = st.get("child", [])
            if len(kids) == 2 and kids[0].get("type") == "LIST" and kids[1].get("type") == "BLOCK":
                args = kids[0].get("child", [])
                name = _string(args[0]) if len(args) == 1 else None
                if name is not None:
                    yield name, kids[1]
        elif t == "CONDITION":
            for sub in st.get("child", [])[1:]:
                if sub.get("type") in ("BLOCK", "CONDITION"):
                    yield from _targets(sub if sub["type"] == "BLOCK" else {"child": [sub]})


def label(file, name):
    d = os.path.dirname(file).replace(os.sep, "/")
    return "//%s" % d if name == os.path.basename(d) else "//%s:%s" % (d, name)


def index(root, gn):
    """({(slot, value): [provider label, ...]}, {slot: [interface label, ...]})
    of the BUILD.gn files under root."""
    providers, interfaces = {}, {}
    for file, tree in dump_trees(root, gn, build_files(root)):
        for name, block in _targets(tree):
            try:
                provides = _assigned(block, KEY)
                interface = _assigned(block, INTERFACE_KEY)
            except ValueError as e:
                sys.exit("link_slot_index: %s, target %r: %s" % (file, name, e))
            for entry in provides:
                slot, _, value = entry.partition("=")
                providers.setdefault((slot, value or name), []).append(label(file, name))
            for slot in interface:
                interfaces.setdefault(slot, []).append(label(file, name))
    return providers, interfaces


def scan(root, gn):
    """{(slot, value): [label, ...]} of the BUILD.gn files under root."""
    return index(root, gn)[0]


def _gn_string(s):
    return '"%s"' % s.replace("\\", "\\\\").replace('"', '\\"').replace("$", "\\$")


def main():
    root, gn = sys.argv[1], sys.argv[2]
    providers, interfaces = index(root, gn)
    orphans = sorted({slot for slot, _ in providers} - set(interfaces))
    if orphans:
        sys.exit("link_slot_index: link slots with providers and no interface (a target with "
                 "`link_slot_interface`, `<header>  # gn: slot <slot>` in its ya.make):\n" +
                 "\n".join("  %s: %s" % (slot, " ".join(sorted(l for (s, _), ls in providers.items()
                                                                if s == slot for l in ls)))
                           for slot in orphans))
    print("providers = [")
    for (slot, value), labels in sorted(providers.items()):
        print("  {\n    slot = %s\n    value = %s\n    labels = [ %s ]\n  }," % (
            _gn_string(slot), _gn_string(value), ", ".join(map(_gn_string, sorted(labels)))))
    print("]")


if __name__ == "__main__":
    main()
