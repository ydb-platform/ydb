#!/usr/bin/env python3
"""CLI for the CI metrics DataLens dashboard: pull, diff, publish, YDB table info."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from checks import check_texts
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

REPO_MARKERS = (".github/config/ydb_qa_config.json", ".git")
USEFUL_SQL = {
    "schema": "SELECT * FROM `analytics/ci_metrics` WHERE 1 = 0 LIMIT 0;",
    "run": (
        "SELECT run_id, run_attempt, source, name, value / 60000.0 AS min,\n"
        "       JSON_VALUE(labels, '$.ya_attempt') AS ya_try\n"
        "FROM `analytics/ci_metrics`\n"
        "WHERE kind = 'duration' AND run_id = 36846043293UL\n"
        "ORDER BY event_ts;"
    ),
    "slow-try1": (
        "SELECT run_id, github_job_id, SUM(value) / 60000.0 AS tests_min\n"
        "FROM `analytics/ci_metrics`\n"
        "WHERE kind = 'duration' AND name = 'ya_tests' AND workflow = 'PR-check'\n"
        "  AND build_preset = 'relwithdebinfo'\n"
        "  AND date >= CurrentUtcDate() - Interval('P7D')\n"
        "  AND JSON_VALUE(labels, '$.ya_attempt') = '1'\n"
        "GROUP BY run_id, github_job_id\n"
        "HAVING tests_min >= 150\n"
        "ORDER BY tests_min DESC;"
    ),
}


def repo_root(start=None):
    cur = Path(start or HERE).resolve()
    for folder in [cur] + list(cur.parents):
        if (folder / ".github/config/ydb_qa_config.json").is_file() or (folder / ".git").exists():
            return folder
    return HERE.parents[4]


def fetch_live(name, spec, token, manifest):
    org = manifest["org_id"]
    workbook = manifest["workbook_id"]
    kind = spec["kind"]
    obj_id = spec["id"]
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


def cmd_show(args, manifest):
    name = args.names[0] if args.names else None
    if not name:
        raise SystemExit("show needs an object name")
    spec = manifest["objects"][name]
    dest = object_dir(name)
    print(json.dumps({"name": name, "kind": spec["kind"], "id": spec["id"], "path": str(dest)}, indent=2))
    if spec["kind"] == "dataset":
        print("--- query.sql ---")
        print(read_dataset(name)["sql"])
    elif spec["kind"] == "chart":
        for fname in ("sources.js", "prepare.js", "meta.json"):
            path = dest / fname
            print("--- %s (%s bytes) ---" % (fname, path.stat().st_size if path.is_file() else 0))
    elif spec["kind"] == "dashboard":
        print("--- entry.json ---")
        print((dest / "entry.json").read_text(encoding="utf-8")[:2000])


def cmd_pull(args, manifest):
    token = iam_token()
    for name in object_names(manifest, args.names):
        spec = manifest["objects"][name]
        payload = fetch_live(name, spec, token, manifest)
        dest = write_live(name, spec["kind"], payload)
        print("pulled %s -> %s" % (name, dest.relative_to(STORE_HERE)))


def cmd_diff(args, manifest):
    token = iam_token()
    diffs = 0
    for name in object_names(manifest, args.names):
        spec = manifest["objects"][name]
        live = live_fingerprint(spec["kind"], fetch_live(name, spec, token, manifest))
        local = local_fingerprint(name, spec["kind"])
        keys = sorted(set(live) | set(local))
        changed = [key for key in keys if live.get(key) != local.get(key)]
        if not changed:
            print("ok  %s" % name)
            continue
        diffs += 1
        print("DIFF %s  (%s)" % (name, ", ".join(changed)))
        for key in changed:
            left = local.get(key)
            right = live.get(key)
            if isinstance(left, str) and isinstance(right, str):
                print("  %s local=%dB live=%dB" % (key, len(left), len(right)))
            else:
                print("  %s changed" % key)
    return 1 if diffs else 0


def cmd_publish(args, manifest):
    errors = check_texts()
    if errors:
        print("local check failed:", file=sys.stderr)
        for item in errors:
            print("  %s" % item, file=sys.stderr)
        return 1
    token = iam_token() if args.apply else None
    org = manifest["org_id"]
    workbook = manifest["workbook_id"]
    for name in object_names(manifest, args.names):
        spec = manifest["objects"][name]
        kind = spec["kind"]
        obj_id = spec["id"]
        if kind == "chart":
            entry = chart_entry(name, obj_id)
            print("chart %s save->publish entryId=%s" % (name, obj_id))
            if args.apply:
                result = publish_editor_chart(token, org, entry, workbook_id=workbook)
                print("  revId %s" % result["revId"])
        elif kind == "dataset":
            dataset = read_dataset(name)["dataset"]
            print("dataset %s updateDataset id=%s" % (name, obj_id))
            if args.apply:
                update_dataset(token, org, obj_id, workbook, dataset)
                print("  updated")
        elif kind == "dashboard":
            entry = dashboard_entry(name, obj_id)
            print("dashboard %s save->publish id=%s" % (name, obj_id))
            if args.apply:
                result = publish_dashboard(token, org, entry, workbook_id=workbook, dashboard_id=obj_id)
                print("  revId %s" % result["revId"])
        else:
            raise SystemExit("unknown kind %s" % kind)
    if not args.apply:
        print("dry-run; pass --apply to write to DataLens")
    return 0


def cmd_check(args, manifest):
    errors = check_texts()
    if not errors:
        print("ok")
        return 0
    for item in errors:
        print(item, file=sys.stderr)
    return 1


def cmd_sql(args, manifest):
    name = args.names[0] if args.names else "duration-ds"
    if manifest["objects"][name]["kind"] != "dataset":
        raise SystemExit("%s is not a dataset" % name)
    sys.stdout.write(read_dataset(name)["sql"])
    if not read_dataset(name)["sql"].endswith("\n"):
        sys.stdout.write("\n")


def load_ydb_config(manifest):
    rel = manifest.get("ydb_config") or ".github/config/ydb_qa_config.json"
    path = repo_root() / rel
    with open(path, encoding="utf-8") as handle:
        cfg = json.load(handle)
    db = cfg["databases"][manifest.get("ydb_database_key") or "main"]
    return db["endpoint"], db["path"], db["tables"]


def ydb_bin():
    return os.environ.get("YDB_CLI") or "ydb"


def ydb_auth_args():
    sa = (
        os.environ.get("YDB_SA_KEY_FILE")
        or os.environ.get("CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS")
        or os.environ.get("ANALYTICS_YDB_CREDENTIALS")
    )
    if sa:
        return ["--sa-key-file", sa], None
    token = os.environ.get("YDB_TOKEN")
    if token:
        return ["--token-file", "/dev/stdin"], token
    try:
        iam = subprocess.check_output(["yc", "iam", "create-token"], text=True).strip()
        return ["--iam-token-file", "/dev/stdin"], iam
    except (OSError, subprocess.CalledProcessError):
        return None, None


def run_ydb(args_tail):
    endpoint, database, _tables = load_ydb_config(load_manifest())
    auth, stdin_token = ydb_auth_args()
    if auth is None:
        print(
            "no YDB credentials. Set YDB_TOKEN, YDB_SA_KEY_FILE, or login with yc.\n"
            "Or run the same SQL via MCP user-ydb-qa / ydb_query.",
            file=sys.stderr,
        )
        return 2
    cmd = [ydb_bin(), "--endpoint", endpoint, "--database", database] + auth + args_tail
    try:
        proc = subprocess.run(cmd, input=stdin_token, text=True, check=False)
    except OSError as exc:
        print("ydb CLI not found (%s). Install ydb or set YDB_CLI." % exc, file=sys.stderr)
        return 2
    return proc.returncode


def cmd_table_info(args, manifest):
    sys.path.insert(0, str(HERE.parent))
    from github_actions.ci_metrics import COLUMNS_SCHEMA, DEFAULT_TABLE_PATH, PRIMARY_KEYS
    from github_actions import taxonomy

    endpoint, database, tables = load_ydb_config(manifest)
    print("table\t%s" % (tables.get("ci_metrics") or DEFAULT_TABLE_PATH))
    print("database\t%s" % database)
    print("endpoint\t%s" % endpoint)
    print("pk\t%s" % ", ".join(PRIMARY_KEYS))
    print("")
    print("columns")
    for name, typ, optional in COLUMNS_SCHEMA:
        print("  %s\t%s%s" % (name, typ, " optional" if optional else ""))
    print("")
    print("sources")
    for source, names in taxonomy.SOURCES.items():
        print("  %s\t%s" % (source, ", ".join(names) or "(dynamic)"))
    print("")
    print("rules")
    print("  one GitHub re-run = same run_id, new run_attempt; treat (run_id, run_attempt) as one run")
    print("  kind=duration; ya_phase comes from ya_evlog_phases.py")
    print("  MCP: user-ydb-qa / ydb_query  (test_results/test_runs_column is often empty for fresh PR-check jobs)")
    print("")
    print("--- sql run ---")
    print(USEFUL_SQL["run"])
    print("--- sql slow-try1 ---")
    print(USEFUL_SQL["slow-try1"])


def cmd_query(args, manifest):
    sql = args.sql
    if args.preset:
        sql = USEFUL_SQL[args.preset]
    if not sql:
        raise SystemExit("query needs SQL or --preset")
    extra = ["sql", "--script", sql, "--format", args.format]
    return run_ydb(extra)


def cmd_describe(args, manifest):
    _endpoint, _database, tables = load_ydb_config(manifest)
    path = args.path or tables.get("ci_metrics") or "analytics/ci_metrics"
    return run_ydb(["scheme", "describe", path])


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    def add_names(cmd, nargs="*"):
        cmd.add_argument("names", nargs=nargs, help="object name from manifest.json")

    add_names(sub.add_parser("list", help="list dashboard / charts / datasets"))
    add_names(sub.add_parser("show", help="print one local object"), nargs=1)
    add_names(sub.add_parser("pull", help="fetch live DataLens into objects/"))
    add_names(sub.add_parser("diff", help="compare local objects/ with live"))
    pub = sub.add_parser("publish", help="save then publish local objects (dry-run unless --apply)")
    pub.add_argument("--apply", action="store_true", help="write to DataLens")
    add_names(pub)
    sub.add_parser("check", help="run local invariants (no network)")
    add_names(sub.add_parser("sql", help="print a dataset query.sql"), nargs="?")
    sub.add_parser("table-info", help="print analytics/ci_metrics schema and sample SQL")
    query = sub.add_parser("query", help="run SQL via ydb CLI")
    query.add_argument("sql", nargs="?", help="YQL text")
    query.add_argument("--preset", choices=sorted(USEFUL_SQL), help="named sample query")
    query.add_argument("--format", default="pretty", help="ydb sql --format (default pretty)")
    desc = sub.add_parser("describe", help="ydb scheme describe")
    desc.add_argument("path", nargs="?", help="table path, default analytics/ci_metrics")
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    manifest = load_manifest()
    if args.command == "sql":
        names = [args.names] if isinstance(args.names, str) else (args.names or [])
        args.names = names
    handlers = {
        "list": cmd_list,
        "show": cmd_show,
        "pull": cmd_pull,
        "diff": cmd_diff,
        "publish": cmd_publish,
        "check": cmd_check,
        "sql": cmd_sql,
        "table-info": cmd_table_info,
        "query": cmd_query,
        "describe": cmd_describe,
    }
    try:
        result = handlers[args.command](args, manifest)
    except DataLensError as exc:
        print(exc, file=sys.stderr)
        return 1
    return 0 if result is None else result


if __name__ == "__main__":
    sys.exit(main())
