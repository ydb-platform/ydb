#!/usr/bin/env python3
import sys
from pathlib import Path

SDK = Path(__file__).resolve().parent.parent
PREFIX = Path("ydb/public/sdk/cpp")
ALLOWLIST = SDK / "allowed_peerdirs.txt"
ALLOWED = [
    line.split("#", 1)[0].strip()
    for line in ALLOWLIST.read_text(encoding="utf-8").splitlines()
    if line.split("#", 1)[0].strip()
]
IN_SCOPE = {"src", "include", "plugins", "examples"}


def ok(rel):
    parts = Path(rel).parts
    return (
        parts
        and parts[0] in IN_SCOPE
        and parts[0] not in {"tests", "adapters"}
        and "ut" not in parts
        and "ut_utils" not in parts
    )


def peerdirs(text):
    in_block = False
    for i, line in enumerate(text.splitlines(), 1):
        s = line.split("#", 1)[0].strip()
        if s.startswith("PEERDIR("):
            in_block = True
            continue
        if in_block:
            if s == ")":
                in_block = False
            elif s:
                yield i, s


def targets(changed_path):
    all_targets = [
        p for p in sorted(SDK.rglob("ya.make")) if ok(p.relative_to(SDK))
    ]
    if not changed_path:
        return all_targets
    changed = [
        Path(line.strip())
        for line in Path(changed_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if PREFIX / "allowed_peerdirs.txt" in changed:
        return all_targets
    out = []
    for path in changed:
        try:
            rel = path.relative_to(PREFIX)
        except ValueError:
            continue
        if path.name != "ya.make" or not ok(rel):
            continue
        out.append(SDK / rel)
    return out


def main():
    files = targets(sys.argv[1] if len(sys.argv) > 1 else None)
    bad = []
    for ya_make in files:
        for line_no, dep in peerdirs(ya_make.read_text(encoding="utf-8")):
            if not any(dep.startswith(p) for p in ALLOWED):
                bad.append(f"{ya_make}:{line_no}: forbidden PEERDIR {dep!r}")
    if bad:
        print("\n".join(bad), file=sys.stderr)
        raise SystemExit(1)
    print(f"ok ({len(files)} ya.make)")


if __name__ == "__main__":
    main()
