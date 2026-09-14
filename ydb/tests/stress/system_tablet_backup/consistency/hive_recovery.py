#!/usr/bin/env python3
"""Repair a failed Hive from backups and available peers, without querying it."""
import argparse
import dataclasses
import json
import pathlib
import shutil
import sys

from . import doctor
from .model import HIVE, TABLET_SLICES, Severity
from .registry import required_tables, run_checks, select_checks
from .report import render_json, render_text
from .sources.http_auth import load_credentials
from .sources.live import DEFAULT_TIMEOUT_SECONDS
from .sources import (
    discover_operation_paths, discover_tenant_hives, discover_versioned_paths,
    load_state, read_live,
)


UNAVAILABLE_REPAIRS = {
    "I20": "live generations from the failed root Hive are unavailable",
}


def check_without_root(state, specs):
    outcomes = run_checks(state, specs)
    return [dataclasses.replace(outcome, skipped_reason=UNAVAILABLE_REPAIRS["I20"])
            if outcome.spec.id == "I20" else outcome for outcome in outcomes]


def restore_blockers(outcomes):
    """Block unresolved Hive findings; SchemeShard-only replay findings stay in the report."""
    blockers = []
    for outcome in outcomes:
        if HIVE not in outcome.spec.requires and outcome.spec.id != "I13":
            continue
        if outcome.failed_reason:
            blockers.append("%s: check failed: %s" % (outcome.spec.id, outcome.failed_reason))
        elif outcome.skipped_reason and "ledger" not in outcome.spec.requires and outcome.spec.id != "I20":
            blockers.append("%s: check skipped: %s" % (outcome.spec.id, outcome.skipped_reason))
        elif any(f.severity >= Severity.WARNING for f in outcome.findings):
            blockers.append("%s: unresolved findings" % outcome.spec.id)
    return blockers


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backup-root", required=True)
    parser.add_argument("--hive-id", type=int, required=True)
    parser.add_argument("--mon-endpoint", required=True)
    parser.add_argument("--mon-credentials-file")
    parser.add_argument("--mon-timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS, metavar="SECONDS",
                        help="positive socket timeout for monitoring requests (default: 30); not a total run deadline")
    parser.add_argument("-v", "--verbose", action="store_true", help="print live request progress to stderr")
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    if args.mon_timeout <= 0:
        parser.error("--mon-timeout must be positive")
    try:
        credentials = load_credentials(args.mon_credentials_file, args.mon_endpoint)
    except (OSError, ValueError) as exc:
        print("error: %s" % exc)
        return 2
    out = pathlib.Path(args.out)
    out.mkdir()
    specs = select_checks()
    needed = {k: set(v) for k, v in required_tables(specs).items() if k in TABLET_SLICES}
    for kind, tables in doctor.REQUIRED_TABLES.items():
        needed.setdefault(kind, set()).update(tables)
    state, notes = load_state(root=args.backup_root, needed_tables=needed)
    state.authoritative_schemeshard = True
    assert len(state.by_type(HIVE)) == 1, "this helper restores one root Hive only"
    assert state.by_type(HIVE)[0].tablet_id == args.hive_id
    paths = dict(discover_operation_paths(state))
    paths.update(discover_versioned_paths(state))
    state.live = read_live(
        args.mon_endpoint, set(discover_tenant_hives(state)) - {args.hive_id}, paths,
        credentials=credentials, timeout=args.mon_timeout,
        progress=(lambda message: print(message, file=sys.stderr, flush=True)) if args.verbose else None,
    )
    notes = list(notes) + ["Hive recovery assumes the SchemeShard backup is current and complete"]
    notes += ["Doctor skips %s repair: %s" % item for item in UNAVAILABLE_REPAIRS.items()]
    outcomes = check_without_root(state, specs)
    incomplete_input = [o for o in outcomes if o.spec.id == "I13"]
    (out / "report-before.json").write_text(json.dumps(render_json(state, outcomes, notes), indent=2))
    print(render_text(state, outcomes, notes))
    plan = doctor.plan(state, outcomes)
    excluded = [edit for edit in plan.edits if edit.tablet_type != HIVE]
    plan.edits = [edit for edit in plan.edits if edit.tablet_type == HIVE]
    repaired = out / "backups"
    if plan.edits:
        doctor.apply(state, plan, out_dir=str(repaired))
    else:
        shutil.copytree(args.backup_root, repaired)
    (out / "plan.json").write_text(json.dumps({
        "applied_edits": [dataclasses.asdict(edit) for edit in plan.edits],
        "excluded_non_hive_edits": [dataclasses.asdict(edit) for edit in excluded],
        "discarded_tail": plan.discarded_tail,
        "unavailable_repairs": UNAVAILABLE_REPAIRS,
        "queried_hive_ids": sorted(state.live.hives),
        "failed_hive_id": args.hive_id,
        "unrepairable_findings": plan.unrepairable,
        "authoritative_schemeshard": True,
    }, indent=2))
    print(doctor.render_plan(plan, applied_to=str(repaired), verbose=True))
    repaired_state, notes = load_state(root=str(repaired), needed_tables=needed)
    repaired_state.live = state.live
    outcomes = check_without_root(repaired_state, specs)
    (out / "report-after.json").write_text(json.dumps(render_json(repaired_state, outcomes, notes), indent=2))
    print(render_text(repaired_state, outcomes, notes))
    blockers = restore_blockers(outcomes) + restore_blockers(incomplete_input)
    if any(not hive.reachable for hive in state.live.hives.values()):
        blockers.append("live tenant Hive data is incomplete")
    (out / "restore-gate.json").write_text(json.dumps({
        "allowed": not blockers, "blockers": blockers,
        "unavailable_repairs": UNAVAILABLE_REPAIRS,
        "scope": "Hive restore; SchemeShard-only findings are reported separately",
    }, indent=2))
    if blockers:
        print("RESTORE BLOCKED: " + "; ".join(blockers))
        return 3
    print("Hive restore gate passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
