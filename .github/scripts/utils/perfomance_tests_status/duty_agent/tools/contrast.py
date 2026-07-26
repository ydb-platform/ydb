"""Compare focus run vs previous sandbox reports (fingerprint interval)."""

from __future__ import annotations

from typing import Any
from urllib.parse import urljoin

from .http_fetch import fetch_json
from .sandbox import _report_base, extract_fingerprints
from .yav import sandbox_oauth_token


INFRA_FPS = frozenset(
    {
        "disconnected_node",
        "node_disconnected",
        "cluster_unavailable",
        "node_down",
        "node_restarted",
        "transport_error",
        "unavailable",
    }
)


def quick_allure_scan(report_url: str, *, oauth: str | None, max_cases: int = 24) -> dict[str, Any]:
    """Allure scan of ALL failed/broken cases (statusMessage only — no attachments).

    Used to inventory problems across prior runs (not sticky-query-only).
    """
    out: dict[str, Any] = {
        "url": report_url,
        "fetched": False,
        "failed_names": [],
        "cases": [],
        "fingerprints": [],
        "primary": None,
        "summary": None,
        "error": None,
    }
    if not report_url:
        out["error"] = "no report url"
        return out
    base = _report_base(report_url)
    try:
        try:
            summary = fetch_json(urljoin(base, "widgets/summary.json"), oauth=oauth)
            if isinstance(summary, dict):
                out["summary"] = summary.get("statistic")
        except Exception as e:  # noqa: BLE001
            out["error"] = f"summary: {e}"

        chart = fetch_json(urljoin(base, "widgets/status-chart.json"), oauth=oauth)
        if not isinstance(chart, list):
            chart = []
        failed = [
            x for x in chart if isinstance(x, dict) and x.get("status") in ("failed", "broken")
        ]
        out["failed_names"] = [str(x.get("name") or "") for x in failed[:max_cases]]
        blobs: list[str] = []
        for item in failed[:max_cases]:
            uid = item.get("uid")
            name = str(item.get("name") or "")
            if not uid:
                out["cases"].append(
                    {
                        "name": name,
                        "statusMessage": "",
                        "fingerprints": [],
                        "primary": None,
                    }
                )
                continue
            try:
                tc = fetch_json(urljoin(base, f"data/test-cases/{uid}.json"), oauth=oauth)
            except Exception as e:  # noqa: BLE001
                out["error"] = f"test-case: {e}"
                out["cases"].append(
                    {
                        "name": name,
                        "statusMessage": "",
                        "fingerprints": [],
                        "primary": None,
                    }
                )
                continue
            if not isinstance(tc, dict):
                continue
            msg = str(tc.get("statusMessage") or "")
            case_fp = extract_fingerprints(msg) if msg else {
                "fingerprints": [],
                "primary": None,
            }
            out["cases"].append(
                {
                    "uid": uid,
                    "name": str(tc.get("name") or name),
                    "status": tc.get("status") or item.get("status"),
                    "statusMessage": msg[:2000],
                    "fingerprints": list(case_fp.get("fingerprints") or []),
                    "primary": case_fp.get("primary"),
                }
            )
            if msg:
                blobs.append(msg)
        fp = extract_fingerprints("\n".join(blobs)) if blobs else {
            "fingerprints": [],
            "primary": None,
        }
        out["fingerprints"] = list(fp.get("fingerprints") or [])
        out["primary"] = fp.get("primary")
        out["fetched"] = True
        if out.get("error") and out["fetched"]:
            # soft errors ok if we got data
            if out["fingerprints"] or out["failed_names"] or out["cases"]:
                out["error"] = None
    except Exception as e:  # noqa: BLE001
        out["error"] = str(e)[:300]
    return out


def _hist_points(hist: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not hist or not isinstance(hist, dict):
        return []
    labels = list(hist.get("labels") or [])
    versions = list(hist.get("versions") or [])
    reports = list(hist.get("reports") or [])
    fr = list(hist.get("fail_rate") or [])
    n = max(len(labels), len(versions), len(reports), len(fr))
    points = []
    for i in range(n):
        points.append(
            {
                "index": i,
                "label": labels[i] if i < len(labels) else None,
                "sha": versions[i] if i < len(versions) else None,
                "report": reports[i] if i < len(reports) else None,
                "fail_rate": fr[i] if i < len(fr) else None,
            }
        )
    return points


def _norm_rate(v: Any) -> float | None:
    if v is None:
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    if x > 1.5:
        x /= 100.0
    return x


def build_contrast(
    ctx: dict[str, Any],
    history: dict[str, Any],
    focus_sandbox: dict[str, Any],
    *,
    offline: bool = False,
    max_prev: int = 3,
) -> dict[str, Any]:
    """Scan prev-green + recent prior reports; see if same fingerprints already existed."""
    appeared = history.get("appeared") or {}
    sticky = (ctx.get("sticky_detail") or {}).get("history")
    suite = ctx.get("suite_history")
    points = _hist_points(sticky if isinstance(sticky, dict) else None)
    if not points:
        points = _hist_points(suite if isinstance(suite, dict) else None)

    focus_fps = set(focus_sandbox.get("fingerprints") or [])
    focus_infra = bool(focus_fps & INFRA_FPS)

    out: dict[str, Any] = {
        "enabled": not offline,
        "focus": {
            "sha": appeared.get("focus_sha"),
            "label": appeared.get("focus_label"),
            "fingerprints": sorted(focus_fps),
            "primary": focus_sandbox.get("primary"),
            "failed_names": list(
                (focus_sandbox.get("allure") or {}).get("failed_names")
                or focus_sandbox.get("failed_names")
                or []
            ),
        },
        "prev_green": None,
        "prior_scans": [],
        "same_class_before": False,
        "same_class_interval": None,
        "sticky_fresh_but_class_old": False,
        "conclusion": None,
        "error": None,
    }
    if offline:
        out["error"] = "offline"
        out["conclusion"] = "Skipped previous-run Allure contrast (offline)."
        return out

    token = sandbox_oauth_token()
    # Indices to scan: prev_green + up to max_prev-1 earlier points with reports
    first_sha = appeared.get("first_fail_sha")
    prev_sha = appeared.get("prev_green_sha")
    prev_report = appeared.get("prev_green_report")

    def find_idx(sha: str | None) -> int | None:
        if not sha:
            return None
        for p in points:
            if p.get("sha") and str(p["sha"]).startswith(str(sha)[:7]):
                return int(p["index"])
        return None

    fi = find_idx(first_sha)
    pi = find_idx(prev_sha)
    scan_idxs: list[int] = []
    if pi is not None:
        for j in range(max(0, pi - (max_prev - 1)), pi + 1):
            scan_idxs.append(j)
    elif prev_report:
        scan_idxs = []

    # unique
    seen: set[int] = set()
    ordered: list[int] = []
    for i in scan_idxs:
        if i not in seen:
            seen.add(i)
            ordered.append(i)

    scans: list[dict[str, Any]] = []
    if ordered:
        for i in ordered:
            p = points[i]
            url = p.get("report")
            if not url:
                continue
            scan = quick_allure_scan(str(url), oauth=token)
            scan["label"] = p.get("label")
            scan["sha"] = p.get("sha")
            scan["fail_rate"] = p.get("fail_rate")
            scan["index"] = i
            scan["infra_fps"] = sorted(set(scan.get("fingerprints") or []) & INFRA_FPS)
            scans.append(scan)
    elif prev_report:
        scan = quick_allure_scan(str(prev_report), oauth=token)
        scan["label"] = appeared.get("prev_green_label")
        scan["sha"] = prev_sha
        scan["fail_rate"] = 0
        scan["infra_fps"] = sorted(set(scan.get("fingerprints") or []) & INFRA_FPS)
        scans.append(scan)

    out["prior_scans"] = [
        {
            "label": s.get("label"),
            "sha": s.get("sha"),
            "url": s.get("url"),
            "fail_rate": s.get("fail_rate"),
            "failed_names": s.get("failed_names"),
            "cases": s.get("cases") or [],
            "fingerprints": s.get("fingerprints"),
            "primary": s.get("primary"),
            "infra_fps": s.get("infra_fps"),
            "summary": s.get("summary"),
            "fetched": s.get("fetched"),
            "error": s.get("error"),
        }
        for s in scans
    ]

    # prev_green detail
    for s in scans:
        if prev_sha and s.get("sha") and str(s["sha"]).startswith(str(prev_sha)[:7]):
            out["prev_green"] = out["prior_scans"][scans.index(s)]
            break
    if out["prev_green"] is None and scans:
        # last scanned before focus is usually prev green
        out["prev_green"] = out["prior_scans"][-1]

    same_before = []
    for s in scans:
        s_fps = set(s.get("fingerprints") or [])
        if focus_infra and (s_fps & INFRA_FPS):
            same_before.append(s)
        elif focus_fps and (s_fps & focus_fps):
            same_before.append(s)

    out["same_class_before"] = bool(same_before)
    if same_before:
        first = same_before[0]
        last = same_before[-1]
        out["same_class_interval"] = {
            "from_label": first.get("label"),
            "from_sha": first.get("sha"),
            "to_label": last.get("label"),
            "to_sha": last.get("sha"),
            "runs_with_same_class": len(same_before),
            "scanned": len(scans),
        }

    sticky_fresh = bool(appeared.get("fresh_on_focus"))
    if sticky_fresh and out["same_class_before"] and focus_infra:
        out["sticky_fresh_but_class_old"] = True
        interval = out["same_class_interval"] or {}
        out["conclusion"] = (
            f"Sticky/query metric looks fresh on focus sha, but the same surface class "
            f"(2005 / node lost) already hit prior runs "
            f"({interval.get('from_sha')} … {interval.get('to_sha')}, "
            f"{interval.get('runs_with_same_class')}/{interval.get('scanned')} scanned). "
            f"'prev-green' here means sticky/query was green — NOT that the suite run was clean."
        )
    elif out["same_class_before"]:
        interval = out["same_class_interval"] or {}
        out["conclusion"] = (
            f"Same surface error class already present before focus "
            f"(from `{interval.get('from_sha')}`)."
        )
    elif scans and focus_infra:
        out["conclusion"] = (
            "Prior scanned runs did not show the same infra fingerprints — "
            "class may be new in this window (or earlier runs were clean)."
        )
    elif not scans:
        out["conclusion"] = "No previous report URLs available to contrast."
        out["error"] = "no prior reports"
    else:
        out["conclusion"] = "Prior runs scanned; fingerprint overlap inconclusive."

    # Always spell out that sticky-green ≠ suite-green when prior Allure has fails
    if out.get("prev_green"):
        pg = out["prev_green"]
        fails = pg.get("failed_names") or []
        if fails:
            out["prev_run_not_clean"] = True
            out["conclusion"] = (
                (out.get("conclusion") or "")
                + f" Prev sticky-green run Allure still FAILED: {fails} "
                f"(url={pg.get('url')})."
            ).strip()

    return out
