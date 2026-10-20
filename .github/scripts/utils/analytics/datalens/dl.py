#!/usr/bin/env python3
"""Pull, diff, and publish the live CI metrics DataLens dashboard."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from rpc import (
    DataLensError,
    get_dashboard,
    get_dataset,
    get_editor_chart,
    iam_token,
    publish_dashboard,
    publish_editor_chart,
    update_dataset,
)
from store import (
    HERE as STORE_HERE,
    chart_entry,
    dashboard_entry,
    live_fingerprint,
    load_manifest,
    local_fingerprint,
    object_dir,
    object_names,
    read_dataset,
    write_live,
)


def fetch_live(name, spec, token, manifest):
    org = manifest["org_id"]
    workbook = manifest["workbook_id"]
    kind = spec["kind"]
    obj_id = spec.get("id")
    if not obj_id:
        raise SystemExit("%s has no id in manifest.json" % name)
    if kind == "chart":
        return get_editor_chart(token, org, obj_id, workbook)
    if kind == "dataset":
        return get_dataset(token, org, obj_id, workbook)
    if kind == "dashboard":
        return get_dashboard(token, org, obj_id, workbook)
    raise SystemExit("unknown kind %s" % kind)


def cmd_list(args, manifest):
    for name in object_names(manifest, args.names):
        spec = manifest["objects"][name]
        dest = object_dir(name)
        mark = "local" if dest.exists() else "missing"
        print("%s\t%s\t%s\t%s\t%s" % (name, spec["kind"], spec["id"], mark, spec.get("title") or ""))


def cmd_pull(args, manifest):
    token = iam_token()
    for name in object_names(manifest, args.names):
        spec = manifest["objects"][name]
        dest = write_live(name, spec["kind"], fetch_live(name, spec, token, manifest))
        print("pulled %s -> %s" % (name, dest.relative_to(STORE_HERE)))


def cmd_diff(args, manifest):
    token = iam_token()
    diffs = 0
    for name in object_names(manifest, args.names):
        spec = manifest["objects"][name]
        live = live_fingerprint(spec["kind"], fetch_live(name, spec, token, manifest))
        local = local_fingerprint(name, spec["kind"])
        changed = [key for key in sorted(set(live) | set(local)) if live.get(key) != local.get(key)]
        if not changed:
            print("ok  %s" % name)
            continue
        diffs += 1
        print("DIFF %s  (%s)" % (name, ", ".join(changed)))
    return 1 if diffs else 0


def cmd_publish(args, manifest):
    token = iam_token() if args.apply else None
    org = manifest["org_id"]
    workbook = manifest["workbook_id"]
    for name in object_names(manifest, args.names):
        spec = manifest["objects"][name]
        kind = spec["kind"]
        obj_id = spec.get("id")
        if not obj_id:
            raise SystemExit("%s has no id in manifest.json" % name)
        if kind == "chart":
            print("chart %s save->publish entryId=%s" % (name, obj_id))
            if args.apply:
                result = publish_editor_chart(token, org, chart_entry(name, obj_id), workbook_id=workbook)
                print("  revId %s" % result["revId"])
        elif kind == "dataset":
            print("dataset %s updateDataset id=%s" % (name, obj_id))
            if args.apply:
                update_dataset(token, org, obj_id, workbook, read_dataset(name)["dataset"])
                print("  updated")
        elif kind == "dashboard":
            print("dashboard %s save->publish id=%s" % (name, obj_id))
            if args.apply:
                result = publish_dashboard(
                    token, org, dashboard_entry(name, obj_id), workbook_id=workbook, dashboard_id=obj_id
                )
                print("  revId %s" % result["revId"])
        else:
            raise SystemExit("unknown kind %s" % kind)
    if not args.apply:
        print("dry-run; pass --apply to write to DataLens")
    return 0


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    def add_names(cmd):
        cmd.add_argument("names", nargs="*", help="object name from manifest.json")

    add_names(sub.add_parser("list", help="list dashboard / charts / datasets"))
    add_names(sub.add_parser("pull", help="fetch live DataLens into objects/"))
    add_names(sub.add_parser("diff", help="compare local objects/ with live"))
    pub = sub.add_parser("publish", help="save then publish local objects (dry-run unless --apply)")
    pub.add_argument("--apply", action="store_true", help="write to DataLens")
    add_names(pub)
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    handlers = {
        "list": cmd_list,
        "pull": cmd_pull,
        "diff": cmd_diff,
        "publish": cmd_publish,
    }
    try:
        result = handlers[args.command](args, load_manifest())
    except DataLensError as exc:
        print(exc, file=sys.stderr)
        return 1
    return 0 if result is None else result


if __name__ == "__main__":
    sys.exit(main())
