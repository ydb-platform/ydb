# -*- coding: utf-8 -*-
"""Rendering of check results."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Sequence

from .model import LEDGER, LIVE, ClusterState, Severity
from .registry import CheckOutcome

_MARK = {
    Severity.INFO: "info",
    Severity.WARNING: "WARN",
    Severity.ERROR: "FAIL",
    Severity.CRITICAL: "CRIT",
}


def summarize(outcomes: Sequence[CheckOutcome]) -> Dict[str, int]:
    counts = {severity.name.lower(): 0 for severity in Severity}
    counts.update({"checks": len(outcomes), "skipped": 0, "broken": 0})
    for outcome in outcomes:
        if outcome.skipped_reason:
            counts["skipped"] += 1
        if outcome.failed_reason:
            counts["broken"] += 1
        for finding in outcome.findings:
            counts[finding.severity.name.lower()] += 1
    return counts


def max_severity(outcomes: Sequence[CheckOutcome]) -> Severity:
    best = Severity.INFO
    for outcome in outcomes:
        for finding in outcome.findings:
            if finding.severity > best:
                best = finding.severity
    return best


def render_text(
    state: ClusterState,
    outcomes: Sequence[CheckOutcome],
    notes: Sequence[str] = (),
    verbose: bool = False,
) -> str:
    lines: List[str] = []

    names = {"hive": "Hive", "scheme_shard": "SchemeShard", "bscontroller": "BSController"}
    for dump in sorted(state.dumps, key=lambda d: (d.tablet_type, d.tablet_id)):
        lines.append("%s data successfully loaded from %s" % (names.get(dump.tablet_type, dump.tablet_type), dump.source))
    if state.ledger is not None:
        lines.append("Ledger data successfully loaded from %s" % state.ledger.source)
    if state.live is not None:
        readings = list(state.live.hives.values()) + list(state.live.paths.values())
        if readings:
            if all(reading.reachable for reading in readings):
                status = "successfully loaded"
            elif any(reading.reachable for reading in readings):
                status = "partially loaded"
            else:
                status = "could not be loaded"
            lines.append("Live data %s from %s" % (status, state.live.source))
        for hive_id, hive in sorted(state.live.hives.items()):
            if not hive.reachable:
                lines.append("WARN  Could not load live Hive %d from %s: %s" % (hive_id, state.live.source, hive.error))
        for path, live_path in sorted(state.live.paths.items()):
            if not live_path.reachable:
                lines.append("WARN  Could not load live path %s from %s: %s" % (path, state.live.source, live_path.error))
    if verbose:
        lines.append("state: %s" % state.describe())
        for dump in sorted(state.dumps, key=lambda d: d.tablet_type):
            lines.append(
                "  %-14s %s  (%d changelog commits) %s"
                % (dump.tablet_type, dump.source, dump.changelog_commits,
                   "[changelog truncated]" if dump.changelog_truncated else "")
            )
    for note in notes:
        lines.append("  note: %s" % note)
    if lines:
        lines.append("")

    skipped: Dict[str, List[str]] = {}
    for outcome in outcomes:
        header = "%-5s %s" % (outcome.spec.id, outcome.spec.title)

        if outcome.skipped_reason:
            if verbose:
                lines.append("SKIP  %s -- %s" % (header, outcome.skipped_reason))
            elif set(outcome.spec.missing_slices(state.slices())) - {LEDGER, LIVE}:
                skipped.setdefault(outcome.skipped_reason, []).append(outcome.spec.id)
            continue
        if outcome.failed_reason:
            lines.append("BROKE %s -- check itself raised: %s" % (header, outcome.failed_reason))
            continue
        if not outcome.findings:
            if verbose:
                lines.append("ok    %s" % header)
            continue

        if not verbose:
            findings = [f for f in outcome.findings if f.severity > Severity.INFO]
            if not findings:
                continue
            worst = max(f.severity for f in findings)
            counts = {
                severity: sum(f.severity == severity for f in findings)
                for severity in reversed(Severity)
            }
            summary = ", ".join(
                "%d %s" % (count, severity.name.lower())
                for severity, count in counts.items() if count
            )
            lines.append("%-5s %s -- %s" % (_MARK[worst], header, summary))
            # Sample each severity separately so frequent warnings cannot hide
            # a critical finding at the end of the check's output.
            for severity, count in counts.items():
                if not count:
                    continue
                examples = [f for f in findings if f.severity == severity][:3]
                for finding in examples:
                    lines.append("        [%s] %s" % (_MARK[severity], finding.message))
                if count > len(examples):
                    lines.append("        ... %d more %s" % (count - len(examples), severity.name.lower()))
            continue

        worst = max(f.severity for f in outcome.findings)
        lines.append("%-5s %s" % (_MARK[worst], header))
        for finding in outcome.findings:
            lines.append("        [%s] %s" % (_MARK[finding.severity], finding.message))
            if verbose and finding.details:
                lines.append("              %s" % json.dumps(finding.details, sort_keys=True))

    for reason, check_ids in skipped.items():
        lines.append("SKIP  %s -- %s" % (", ".join(check_ids), reason))

    counts = summarize(outcomes)
    if lines and lines[-1]:
        lines.append("")
    summary = "%d checks: %d critical, %d error, %d warning" % (
        counts["checks"], counts["critical"], counts["error"], counts["warning"]
    )
    if verbose:
        summary += ", %d info, %d skipped, %d broken" % (counts["info"], counts["skipped"], counts["broken"])
    lines.append(summary)

    return "\n".join(lines)


def render_json(
    state: ClusterState,
    outcomes: Sequence[CheckOutcome],
    notes: Sequence[str] = (),
) -> Dict[str, Any]:
    return {
        "state": {
            "tablets": [
                {
                    "tablet_type": d.tablet_type,
                    "tablet_id": d.tablet_id,
                    "generation": d.generation,
                    "step": d.step,
                    "snapshot_started_at": d.snapshot_started_at,
                    "changelog_mtime": d.changelog_mtime,
                    "source": d.source,
                    "changelog_commits": d.changelog_commits,
                    "changelog_truncated": d.changelog_truncated,
                }
                for d in sorted(state.dumps, key=lambda d: d.tablet_type)
            ],
            "ledger_entries": len(state.ledger) if state.ledger else None,
        },
        "notes": list(notes),
        "checks": [outcome.to_dict() for outcome in outcomes],
        "summary": summarize(outcomes),
        "max_severity": max_severity(outcomes).name,
    }
