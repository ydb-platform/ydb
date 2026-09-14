# -*- coding: utf-8 -*-
"""Command line entry point.

Production use -- collect the backups from the hosts, then::

    python3 -m consistency --backup-root /tablet

or, when the backups were gathered into arbitrary directories::

    python3 -m consistency \
        --tablet hive=/tmp/hive/backup_20251007181003Z_g214_s1222 \
        --tablet scheme_shard=/tmp/ss/backup_20251007180500Z_g88_s901

Exit codes: 0 clean, 1 findings at or above ``--fail-on``, 2 could not run.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Dict, List, Sequence

from . import doctor
from .sources.http_auth import load_credentials
from .sources.live import DEFAULT_TIMEOUT_SECONDS
from .model import TABLET_SLICES, Severity
from .registry import required_tables, run_checks, select_checks
from .report import max_severity, render_json, render_text
from .sources import (
    BackupError,
    discover_operation_paths,
    discover_tenant_hives,
    discover_versioned_paths,
    load_ledger,
    load_state,
    read_live,
)


def _parse_tablet_args(values: Sequence[str]) -> Dict[str, str]:
    parsed: Dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise argparse.ArgumentTypeError(
                "--tablet expects <type>=<path>, got %r (types: %s)"
                % (value, ", ".join(TABLET_SLICES))
            )
        tablet_type, path = value.split("=", 1)
        tablet_type = tablet_type.strip()
        if tablet_type not in TABLET_SLICES:
            raise argparse.ArgumentTypeError(
                "unknown tablet type %r (expected one of %s)"
                % (tablet_type, ", ".join(TABLET_SLICES))
            )
        parsed[tablet_type] = path.strip()
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="consistency",
        description="Cross-tablet consistency checks for YDB cluster system tablets.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--backup-root",
        help="directory from system_tablet_backup_config.filesystem.path; "
             "the freshest complete backup per tablet is used",
    )
    parser.add_argument(
        "--tablet",
        action="append",
        default=[],
        metavar="TYPE=PATH",
        help="explicit backup directory for one tablet type, overrides discovery; repeatable",
    )
    parser.add_argument("--ledger", help="workload ledger (JSONL); enables the ledger-backed checks")
    parser.add_argument(
        "--mon-endpoint",
        help="monitoring endpoint of any node, e.g. http://host:8765. Tenant SchemeShards and "
             "tenant Hives have no backups, so this is the only way to see the ids they hold; "
             "without it the tenant checks are skipped",
    )
    parser.add_argument(
        "--tenant-hive",
        action="append",
        default=[],
        type=int,
        metavar="TABLET_ID",
        help="read this Hive live in addition to the ones the SchemeShard backup names; repeatable",
    )
    parser.add_argument(
        "--insecure", action="store_true", help="do not verify TLS certificates of --mon-endpoint"
    )
    parser.add_argument("--mon-credentials-file", metavar="FILE",
                        help="private JSON file with Authorization/Cookie HTTP headers; requires verified HTTPS or loopback")
    parser.add_argument("--mon-timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS, metavar="SECONDS",
                        help="positive socket timeout for monitoring requests (default: 30); not a total run deadline")

    parser.add_argument("--only", action="append", default=[], metavar="ID", help="run only these checks")
    parser.add_argument("--exclude", action="append", default=[], metavar="ID", help="skip these checks")
    parser.add_argument("--tag", action="append", default=[], help="run only checks carrying this tag")

    parser.add_argument(
        "--fail-on",
        default="error",
        help="minimum severity that makes the run fail: info, warning, error, critical (default: error)",
    )
    parser.add_argument("--json", dest="json_path", help="write the full report as JSON to this path")
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="print live request progress to stderr, all checks, findings with details, backup sources and doctor edits",
    )
    parser.add_argument(
        "--skip-checksum-validation",
        action="store_true",
        help="do not verify snapshot sha256; needed when the backup was edited by hand",
    )
    parser.add_argument(
        "--no-changelog",
        action="store_true",
        help="check the snapshot alone, without replaying the changelog on top",
    )
    parser.add_argument("--list-checks", action="store_true", help="list registered checks and exit")

    doctor_group = parser.add_argument_group("doctor mode")
    doctor_group.add_argument(
        "--doctor",
        action="store_true",
        help="propose repairs for the findings that can be fixed in the backup files; "
             "dry run unless --doctor-out or --in-place is given",
    )
    doctor_group.add_argument(
        "--doctor-out",
        metavar="DIR",
        help="copy the affected backups into DIR and repair the copies (recommended)",
    )
    doctor_group.add_argument(
        "--in-place",
        action="store_true",
        help="repair the original backup directories instead of a copy",
    )
    return parser


def _list_checks() -> int:
    for spec in select_checks():
        requires = ", ".join(spec.requires) or "-"
        tags = ", ".join(spec.tags) or "-"
        print("%-5s %s" % (spec.id, spec.title))
        print("      requires: %s | tags: %s" % (requires, tags))
        if spec.description:
            first_line = spec.description.splitlines()[0]
            print("      %s" % first_line)
    return 0


def main(argv: Sequence[str] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.mon_timeout <= 0:
        parser.error("--mon-timeout must be positive")

    if args.list_checks:
        return _list_checks()

    if not args.backup_root and not args.tablet:
        parser.error("nothing to read: pass --backup-root and/or --tablet")

    try:
        credentials = load_credentials(args.mon_credentials_file, args.mon_endpoint, args.insecure)
    except (OSError, ValueError) as exc:
        sys.stderr.write("error: %s\n" % exc)
        return 2

    try:
        fail_on = Severity.parse(args.fail_on)
    except ValueError as exc:
        parser.error(str(exc))

    try:
        explicit = _parse_tablet_args(args.tablet)
        specs = select_checks(only=args.only, exclude=args.exclude, tags=args.tag)
    except (ValueError, argparse.ArgumentTypeError) as exc:
        sys.stderr.write("error: %s\n" % exc)
        return 2

    if not specs:
        sys.stderr.write("error: the filters selected no checks\n")
        return 2

    needed = required_tables(specs)
    # A ledger requirement carries no tables and must not reach the backup loader.
    needed_tables = {slice_: tables for slice_, tables in needed.items() if slice_ in TABLET_SLICES}

    if args.doctor:
        # Repairs read more than the checks that trigger them.
        for slice_, tables in doctor.REQUIRED_TABLES.items():
            if slice_ in needed_tables:
                needed_tables[slice_] = set(needed_tables[slice_]) | set(tables)

    try:
        state, notes = load_state(
            root=args.backup_root,
            explicit=explicit,
            needed_tables=needed_tables,
            apply_changelog=not args.no_changelog,
            verify_checksums=not args.skip_checksum_validation,
        )
    except BackupError as exc:
        sys.stderr.write("error: %s\n" % exc)
        return 2

    notes: List[str] = list(notes)

    if args.ledger:
        ledger = load_ledger(args.ledger)
        if ledger is None:
            notes.append("ledger %s not found, ledger-backed checks will be skipped" % args.ledger)
        else:
            state.ledger = ledger

    source_notes = list(notes)
    if args.mon_endpoint:
        tenant_hives = discover_tenant_hives(state)
        # Read backed-up Hives too.  Doctor needs the running root Hive to
        # distinguish stale rows that would be resurrected from legitimate
        # tablets omitted by a differently-aged SchemeShard backup.
        backed_up_hives = {d.tablet_id for d in state.by_type("hive")}
        hive_ids = set(tenant_hives) | set(args.tenant_hive) | backed_up_hives
        versioned_paths = discover_versioned_paths(state)
        # Paths of in-flight operations are read for their identity only, so a
        # path that is also a versioned object keeps the richer kind.
        live_paths = dict(discover_operation_paths(state))
        live_paths.update(versioned_paths)
        if hive_ids or live_paths:
            state.live = read_live(
                args.mon_endpoint, hive_ids, live_paths, timeout=args.mon_timeout,
                insecure=args.insecure, credentials=credentials,
                progress=(lambda message: print(message, file=sys.stderr, flush=True)) if args.verbose else None,
            )
        if hive_ids:
            named = ", ".join(
                "%d (%s)" % (h, tenant_hives.get(h, "explicit")) for h in sorted(hive_ids)
            )
            notes.append("read %d live Hive(s): %s" % (len(hive_ids), named))
        else:
            notes.append(
                "no tenant Hives found in the SchemeShard backup; pass --tenant-hive to name one"
            )
        if versioned_paths:
            notes.append(
                "read %d live versioned path(s): %s"
                % (len(versioned_paths), ", ".join(sorted(versioned_paths)[:5]))
            )
        operation_paths = len(live_paths) - len(versioned_paths)
        if operation_paths > 0:
            notes.append(
                "read %d live path(s) targeted by operations still in flight in the backup"
                % operation_paths
            )

    if not state.dumps:
        sys.stderr.write("error: no tablet state could be loaded\n")
        for note in notes:
            sys.stderr.write("  note: %s\n" % note)
        return 2

    outcomes = run_checks(state, specs)

    print(render_text(state, outcomes, notes if args.verbose else source_notes, verbose=args.verbose))

    if args.json_path:
        with open(args.json_path, "w") as handle:
            json.dump(render_json(state, outcomes, notes), handle, indent=2, sort_keys=True)
            handle.write("\n")

    if args.doctor:
        print("")
        repair_plan = doctor.plan(state, outcomes)

        if repair_plan.empty or not (args.doctor_out or args.in_place):
            print(doctor.render_plan(repair_plan, verbose=args.verbose))
            return 1 if max_severity(outcomes) >= fail_on else 0

        try:
            written = doctor.apply(
                state, repair_plan, out_dir=args.doctor_out, in_place=args.in_place
            )
        except (ValueError, OSError) as exc:
            sys.stderr.write("error: doctor could not apply repairs: %s\n" % exc)
            return 2

        print(doctor.render_plan(repair_plan, applied_to=written, verbose=args.verbose))
        print("")
        print("re-run the checks against the repaired backup to confirm:")
        print("  python3 -m consistency --backup-root %s" % (args.doctor_out or "<original root>"))
        return 0

    return 1 if max_severity(outcomes) >= fail_on else 0


if __name__ == "__main__":
    sys.exit(main())
