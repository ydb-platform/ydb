"""Find when a pattern behavior started (first occurrence: date, commit, PR)."""
import datetime
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from .patterns import _parse_date


def find_behavior_start(
    match: Dict[str, Any],
    pattern: str,
    runs: List[Dict[str, Any]],
    params: Dict[str, Any],
) -> Tuple[Optional[datetime.date], Optional[str], Optional[str]]:
    """
    Find first occurrence of the pattern behavior in runs.
    Returns (behavior_start_date, behavior_start_commit, behavior_start_pr).
    """
    if not runs:
        return None, None, None

    def _to_date(ts):
        return _parse_date(ts) if ts else None

    def _first(runs_sorted, pred):
        for r in runs_sorted:
            if pred(r):
                d = _to_date(r.get("run_timestamp"))
                commit = r.get("commit") or r.get("commit_sha")
                pull_raw = r.get("pull")
                pull = str(pull_raw) if pull_raw is not None else None
                return d, commit, pull
        return None, None, None

    if pattern == "duration_increased":
        full_name = match.get("full_name", "")
        growth_factor = params.get("growth_factor", 1.5)
        baseline_median = match.get("baseline_median")
        if not baseline_median or baseline_median <= 0:
            return None, None, None
        threshold = baseline_median * growth_factor
        relevant = [r for r in runs if (r.get("full_name") or "") == full_name and r.get("duration")]
        relevant.sort(key=lambda r: (r.get("run_timestamp") or 0))
        return _first(relevant, lambda r: float(r.get("duration", 0) or 0) >= threshold)

    if pattern == "muted_test_different_error":
        full_name = match.get("full_name", "")
        err = (match.get("error_type") or "").strip()
        if not err:
            return None, None, None
        relevant = [
            r
            for r in runs
            if (r.get("full_name") or "") == full_name
            and (r.get("status") or "").lower() in ("failure", "error")
            and (r.get("error_type") or "").strip() == err
        ]
        relevant.sort(key=lambda r: (r.get("run_timestamp") or 0))
        return _first(relevant, lambda _: True)

    if pattern == "floating_across_days":
        suite = match.get("suite_folder", "")
        relevant = [
            r
            for r in runs
            if (r.get("suite_folder") or "") == suite
            and (r.get("status") or "").lower() in ("failure", "error")
            and ("TIMEOUT" in (r.get("error_type") or "").upper())
        ]
        relevant.sort(key=lambda r: (r.get("run_timestamp") or 0))
        return _first(relevant, lambda _: True)

    if pattern == "retry_recovered":
        full_name = match.get("full_name", "")
        job_id = match.get("job_id")
        by_job = defaultdict(list)
        for r in runs:
            if (r.get("full_name") or "") == full_name:
                jid = r.get("job_id")
                if jid is not None:
                    by_job[jid].append((r.get("run_timestamp"), (r.get("status") or "").lower()))
        for jid, events in by_job.items():
            if job_id is not None and jid != job_id:
                continue
            events.sort(key=lambda x: x[0])
            statuses = [s for _, s in events]
            if len(statuses) >= 2:
                first, rest = statuses[0], statuses[1:]
                if first in ("failure", "error") and any(s in ("passed", "ok") for s in rest):
                    run_ts = events[0][0]
                    r0 = next((r for r in runs if r.get("job_id") == jid and r.get("full_name") == full_name), None)
                    if r0:
                        pull_raw = r0.get("pull")
                        pull = str(pull_raw) if pull_raw is not None else None
                        return _to_date(run_ts), r0.get("commit"), pull
        return None, None, None

    return None, None, None
