#!/usr/bin/env python3
"""Resolve every cherry-pick conflict by keeping the commit being applied.

The text between ======= and >>>>>>> replaces the whole hunk. The file is
rewritten in place.

Usage:
    python3 resolve_log_conflicts.py FILE [FILE ...]

Prints "resolved PATH N" for each hunk.
Prints "clean PATH" when the file has no conflict markers.
Exit code 2 means a malformed conflict or an unreadable file.
"""
import re
import sys

OURS_RE = re.compile(r"^<{7}(?:\s|$)")
BASE_RE = re.compile(r"^\|{7}(?:\s|$)")
SEP_RE = re.compile(r"^={7}$")
THEIRS_RE = re.compile(r"^>{7}(?:\s|$)")


class MalformedConflict(Exception):
    pass


def split_regions(text):
    """Yield ('text', chunk) and ('conflict', ours, theirs, raw) in order."""
    lines = text.splitlines(keepends=True)
    i = 0
    while i < len(lines):
        if not OURS_RE.match(lines[i].rstrip("\r\n")):
            start = i
            while i < len(lines) and not OURS_RE.match(lines[i].rstrip("\r\n")):
                i += 1
            yield ("text", "".join(lines[start:i]))
            continue
        start = i
        i += 1
        ours = _take_until(lines, i, (SEP_RE, BASE_RE), "conflict start")
        i = ours[1]
        if i < len(lines) and BASE_RE.match(lines[i].rstrip("\r\n")):
            i += 1
            skipped = _take_until(lines, i, (SEP_RE,), "conflict base")
            i = skipped[1]
        if i >= len(lines) or not SEP_RE.match(lines[i].rstrip("\r\n")):
            raise MalformedConflict("missing =======")
        i += 1
        theirs = _take_until(lines, i, (THEIRS_RE,), "conflict end")
        i = theirs[1]
        if i >= len(lines) or not THEIRS_RE.match(lines[i].rstrip("\r\n")):
            raise MalformedConflict("missing >>>>>>>")
        i += 1
        raw = "".join(lines[start:i])
        yield ("conflict", "".join(ours[0]), "".join(theirs[0]), raw)


def _take_until(lines, i, stops, what):
    taken = []
    while i < len(lines) and not any(regex.match(lines[i].rstrip("\r\n")) for regex in stops):
        if OURS_RE.match(lines[i].rstrip("\r\n")) or THEIRS_RE.match(lines[i].rstrip("\r\n")):
            raise MalformedConflict("nested marker in %s" % what)
        taken.append(lines[i])
        i += 1
    return taken, i


def resolve_text(text):
    """Return (new_text, hunks). Each hunk is (number, 'resolved')."""
    out = []
    hunks = []
    number = 0
    for region in split_regions(text):
        if region[0] == "text":
            out.append(region[1])
            continue
        number += 1
        hunks.append((number, "resolved"))
        out.append(region[2])
    return "".join(out), hunks


def resolve_file(path):
    """Resolve path in place. Return hunks. Raise MalformedConflict or OSError."""
    with open(path, "r", encoding="utf-8") as handle:
        text = handle.read()
    new, hunks = resolve_text(text)
    if new != text:
        with open(path, "w", encoding="utf-8", newline="") as handle:
            handle.write(new)
    return hunks


def main(argv):
    if len(argv) < 2:
        print("usage: resolve_log_conflicts.py FILE [FILE ...]", file=sys.stderr)
        return 2
    status = 0
    for path in argv[1:]:
        try:
            hunks = resolve_file(path)
        except MalformedConflict as exc:
            print("malformed %s %s" % (path, exc), file=sys.stderr)
            status = 2
            continue
        except OSError as exc:
            print("error %s %s" % (path, exc), file=sys.stderr)
            status = 2
            continue
        if not hunks:
            print("clean %s" % path)
            continue
        for number, action in hunks:
            print("%s %s %d" % (action, path, number))
    return status


if __name__ == "__main__":
    sys.exit(main(sys.argv))
