"""Build / merge perf-duty-result/v1."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .run_dir import context_stub, make_run_id, read_json, write_json

RESULT_SCHEMA = "perf-duty-result/v1"
RESOLUTIONS = frozenset(
    {
        "update_known",
        "open_ticket",
        "wait_next_wave",
        "investigate_further",
        "no_action",
        "unknown",
    }
)


def empty_result(
    ctx: dict[str, Any] | None = None,
    *,
    run_id: str | None = None,
    analysis_types: list[str] | None = None,
) -> dict[str, Any]:
    ctx = ctx or {}
    return {
        "schema": RESULT_SCHEMA,
        "ok": True,
        "run_id": run_id or (make_run_id(ctx) if ctx else "duty-run"),
        "context": context_stub(ctx) if ctx else {},
        "analysis_types": list(analysis_types or []),
        "status": "partial",
        "resolution": None,
        "summary": None,
        "confidence": None,
        "confidence_score": None,
        "culprit_found": False,
        "culprit": None,
        "problems": {"total": 0, "analyzed": 0, "unknown": 0, "items": []},
        "errors": [],
        "warnings": [],
        "artifacts": {},
        "timings_sec": {},
    }


def load_problems(out_dir: Path) -> list[dict[str, Any]]:
    p = out_dir / "problems.json"
    if not p.is_file():
        # fall back to seed from detect-type
        det = out_dir / "detect_type.json"
        if det.is_file():
            data = read_json(det)
            return list(data.get("problems_seed") or [])
        return []
    data = read_json(p)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return list(data.get("items") or data.get("problems") or [])
    return []


def _rollup_resolution(items: list[dict[str, Any]]) -> str | None:
    order = [
        "open_ticket",
        "update_known",
        "investigate_further",
        "wait_next_wave",
        "unknown",
        "no_action",
    ]
    present = {str(i.get("resolution") or "") for i in items}
    for r in order:
        if r in present:
            return r
    return None


def _rollup_confidence(items: list[dict[str, Any]]) -> tuple[str | None, float | None]:
    rank = {"low": 0, "medium": 1, "high": 2}
    best_s = None
    best_r = -1
    scores = []
    for i in items:
        c = str(i.get("confidence") or "").lower()
        if c in rank and rank[c] > best_r:
            best_r = rank[c]
            best_s = c
        if isinstance(i.get("confidence_score"), (int, float)):
            scores.append(float(i["confidence_score"]))
    score = sum(scores) / len(scores) if scores else None
    return best_s, score


def merge_result(
    out_dir: Path,
    *,
    ctx: dict[str, Any] | None = None,
    status: str | None = None,
    ok: bool | None = None,
    errors: list[dict[str, Any]] | None = None,
    warnings: list[str | dict[str, Any]] | None = None,
    summary: str | None = None,
    resolution: str | None = None,
    confidence: str | None = None,
) -> dict[str, Any]:
    """Merge problems.json + existing result.json → result.json."""
    path = out_dir / "result.json"
    if path.is_file():
        result = read_json(path)
        if not isinstance(result, dict):
            result = empty_result(ctx)
    else:
        det_types = []
        det_path = out_dir / "detect_type.json"
        if det_path.is_file():
            det = read_json(det_path)
            det_types = list(det.get("analysis_types") or [])
        result = empty_result(ctx, run_id=out_dir.name, analysis_types=det_types)

    if ctx:
        result["context"] = context_stub(ctx)
        result["run_id"] = result.get("run_id") or make_run_id(ctx)

    items = load_problems(out_dir)
    analyzed = 0
    unknown = 0
    culprit = None
    culprit_found = False
    for it in items:
        st = str(it.get("status") or "")
        if st in ("analyzed", "unknown", "done"):
            analyzed += 1
        if st == "unknown" or str(it.get("resolution") or "") in ("unknown", "investigate_further"):
            if st == "unknown" or it.get("resolution") == "unknown":
                unknown += 1
        if it.get("culprit_found") and it.get("culprit"):
            culprit_found = True
            culprit = it.get("culprit")

    result["problems"] = {
        "total": len(items),
        "analyzed": analyzed,
        "unknown": unknown,
        "items": items,
    }

    if resolution:
        result["resolution"] = resolution
    elif not result.get("resolution"):
        result["resolution"] = _rollup_resolution(items)

    conf, score = _rollup_confidence(items)
    if confidence:
        result["confidence"] = confidence
    elif conf:
        result["confidence"] = conf
    if score is not None:
        result["confidence_score"] = score

    result["culprit_found"] = bool(culprit_found or result.get("culprit_found"))
    if culprit is not None:
        result["culprit"] = culprit

    if summary is not None:
        result["summary"] = summary
    if status is not None:
        result["status"] = status
    elif analyzed and analyzed >= len(items) and items:
        result["status"] = "completed"
    if ok is not None:
        result["ok"] = ok

    if errors:
        err_list = list(result.get("errors") or [])
        err_list.extend(errors)
        result["errors"] = err_list
        if any(not e.get("retriable") for e in errors if isinstance(e, dict)):
            result["ok"] = False
            if result.get("status") not in ("stopped",):
                result["status"] = "failed"

    if warnings:
        w_list = list(result.get("warnings") or [])
        for w in warnings:
            w_list.append(w if isinstance(w, dict) else {"message": str(w)})
        result["warnings"] = w_list

    arts = dict(result.get("artifacts") or {})
    for name in (
        "analysis.md",
        "focus.json",
        "priors.json",
        "detect_type.json",
        "problems.json",
        "code_bisect.json",
        "metrics_delta.json",
        "fatal_scan.json",
        "issues.json",
        "action_tree.json",
        "baseline_focus.json",
        "dig_runs.json",
        "dig_prs.json",
    ):
        if (out_dir / name).is_file():
            arts[name.replace(".", "_").replace("json", "json").replace("md", "md")] = name
            # simpler keys
            key = name.replace(".", "_")
            arts[key] = name
    if (out_dir / "analysis.md").is_file():
        arts["analysis_md"] = "analysis.md"
    result["artifacts"] = arts
    result["schema"] = RESULT_SCHEMA

    write_json(path, result)
    return result
