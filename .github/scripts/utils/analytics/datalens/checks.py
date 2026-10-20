#!/usr/bin/env python3
"""Local invariants for CI metrics DataLens objects. No network."""

from __future__ import annotations

import json
import re

from store import collect_texts


IN_PROGRESS_COALESCE = re.compile(r"COALESCE\s*\([^)]*in_progress", re.I)
RUN_LABEL_DATE = "Unicode::Substring(CAST(s.started_at AS Utf8), 0, 10)"
LEXICAL_JOB_CONCLUSION = "MAX(conclusion) AS job_conclusion"
INTERVAL_SPLIT = "indexOf(marker, first + marker.length)"
LOCAL_TZ_GETTERS = re.compile(r"(?<!UTC)get(?:Hours|Day|Month|FullYear|Date|Minutes)\(")


def split_interval(raw):
    """Keep complete __relative_* tokens from a DataLens interval param."""
    value = str(raw or "")
    if not value.startswith("__interval_"):
        return None
    body = value[len("__interval_"):]
    marker = "__relative_"
    first = body.find(marker)
    second = body.find(marker, first + len(marker)) if first >= 0 else -1
    if first == 0 and second >= 0:
        return body[:second].rstrip("_"), body[second:]
    if first > 0:
        return body[:first].rstrip("_"), body[first:]
    t_at = body.find("T")
    split_at = body.find("_", t_at) if t_at >= 0 else -1
    if split_at < 0:
        return None
    return body[:split_at], body[split_at + 1:]


def _has_local_tz_getter(js):
    return bool(LOCAL_TZ_GETTERS.search(js or ""))


def _walk_nodes(node, found):
    if isinstance(node, dict):
        if node.get("id") == "seljobstatus":
            found.append(node)
        for value in node.values():
            _walk_nodes(value, found)
    elif isinstance(node, list):
        for item in node:
            _walk_nodes(item, found)


def _seljobstatus_default(dash_text):
    try:
        payload = json.loads(dash_text)
    except ValueError:
        return None
    widgets = []
    _walk_nodes(payload, widgets)
    for widget in widgets:
        source = widget.get("source") or {}
        if "defaultValue" in source:
            return source["defaultValue"]
    return None


def check_texts(texts=None):
    """Return a list of 'name: message' errors. Empty means OK."""
    errors = []
    texts = collect_texts() if texts is None else texts

    duration_sql = (texts.get("duration-ds") or {}).get("sql") or ""
    if duration_sql:
        if "AS ci_run_id" not in duration_sql:
            errors.append("duration-ds: Duration dataset must expose ci_run_id, not run_id")
        if re.search(r"\sAS run_id\b", duration_sql) and "ci_run_id" not in duration_sql:
            errors.append("duration-ds: do not alias the Duration run column as run_id")
        if "CAST(t.run_attempt" not in duration_sql:
            errors.append("duration-ds: Duration SQL must select run_attempt")
        if IN_PROGRESS_COALESCE.search(duration_sql):
            errors.append("duration-ds: do not COALESCE job_conclusion to in_progress")
        if LEXICAL_JOB_CONCLUSION in duration_sql:
            errors.append("duration-ds: do not use lexical MAX(conclusion) for job_conclusion")
        if "github_step" in duration_sql and "name IN ('Checkout', 'Build and test')" in duration_sql:
            errors.append("duration-ds: do not drop github_step names that Duration/p-diff selectors offer")
        if "github_step" in duration_sql and "NOT LIKE 'Post %'" not in duration_sql:
            errors.append("duration-ds: keep github_step rows except Post %")

    for name in ("gantt-ds", "pickers-ds"):
        sql = (texts.get(name) or {}).get("sql") or ""
        if not sql:
            continue
        if RUN_LABEL_DATE not in sql:
            errors.append("%s: run_label must start with YYYY-MM-DD from started_at" % name)
        if " · #" not in sql and "|| ' · #'" not in sql:
            errors.append("%s: run_label must end with · #<attempt>" % name)
        if "ON s.run_id = t.run_id AND s.run_attempt = t.run_attempt" not in sql:
            errors.append("%s: started_at join must be (run_id, run_attempt)" % name)
        if "ON rs.run_id = t.run_id AND rs.run_attempt = t.run_attempt" not in sql:
            errors.append("%s: run_icon join must be (run_id, run_attempt)" % name)
        if IN_PROGRESS_COALESCE.search(sql):
            errors.append("%s: do not COALESCE job_conclusion to in_progress" % name)
        if re.search(r"COALESCE\(rs\.run_icon,\s*'⚪'\)\s*\|\|", sql):
            errors.append("%s: do not prefix run_label with the status icon" % name)
        if LEXICAL_JOB_CONCLUSION in sql:
            errors.append("%s: do not use lexical MAX(conclusion) for job_conclusion" % name)

    gantt_js = (texts.get("gantt") or {}).get("prepare") or ""
    if gantt_js:
        for marker in ("parseRunRef", "keepOneAttempt"):
            if marker not in gantt_js:
                errors.append("gantt: prepare.js must define %s" % marker)
        if "run_label" not in gantt_js:
            errors.append("gantt: selector run_label must beat gantt_run when choosing a run")
    gantt_src = (texts.get("gantt") or {}).get("sources") or ""
    if gantt_src and "console.log" in gantt_src:
        errors.append("gantt: sources.js must not console.log dashboard params")
    if gantt_js and "console.log('[prepare] start params'" in gantt_js:
        errors.append("gantt: prepare.js must not console.log dashboard params")

    duration_js = (texts.get("duration") or {}).get("prepare") or ""
    if duration_js:
        if "ci_run_id" not in duration_js:
            errors.append("duration: prepare.js must read ci_run_id")
        if "run_attempt" not in duration_js:
            errors.append("duration: prepare.js must group points by run_attempt")
        if "run_label: [String(point.runId" in duration_js:
            errors.append("duration: do not write a short run_id · #attempt into run_label (empties the PR selector)")
        if _has_local_tz_getter(duration_js):
            errors.append("duration: bucket keys must use UTC getters")

    pdiff_js = (texts.get("pdiff") or {}).get("prepare") or ""
    if pdiff_js:
        if "ci_run_id" not in pdiff_js:
            errors.append("pdiff: prepare.js must read ci_run_id")
        if "run_attempt" not in pdiff_js:
            errors.append("pdiff: prepare.js must group by run_attempt")
        if _has_local_tz_getter(pdiff_js):
            errors.append("pdiff: bucket keys must use UTC getters")
    pdiff_src = (texts.get("pdiff") or {}).get("sources") or ""
    if pdiff_src and "'run_attempt'" not in pdiff_src:
        errors.append("pdiff: sources.js must select run_attempt")

    for name in ("duration", "pdiff", "capacity-use", "capacity-pool", "capacity-tight", "capacity-queue"):
        src = (texts.get(name) or {}).get("sources") or ""
        if not src:
            continue
        if INTERVAL_SPLIT not in src:
            errors.append("%s: splitInterval must keep complete __relative_ tokens" % name)
        if "event_date" in src and "GTE" not in src:
            errors.append("%s: apply date bounds in sources.js before the row limit" % name)

    forbidden_html = (
        ('data-wf=', 'DataLens rejects data-wf; use data-id="wf:..."'),
        ('data-busy=', 'DataLens rejects data-busy; use data-id="busy:..."'),
        ('data-wait=', 'DataLens rejects data-wait; use data-id="wait:..."'),
        ('<details open', 'DataLens rejects the open attribute on details'),
        (' rel=', 'DataLens rejects rel= on links'),
    )
    for name, payload in texts.items():
        for body in (payload or {}).values():
            if not isinstance(body, str):
                continue
            for needle, message in forbidden_html:
                if needle in body:
                    errors.append("%s: %s" % (name, message))

    dash = (texts.get("dashboard") or {}).get("dashboard") or ""
    if dash:
        defaults = _seljobstatus_default(dash)
        if defaults is not None and list(defaults) != ["success", "failure"]:
            errors.append("dashboard: job status selector default must be success+failure")
        if IN_PROGRESS_COALESCE.search(dash):
            errors.append("dashboard: do not invent in_progress via COALESCE")
        if '"id": "selrun"' in dash and "captab01" not in dash:
            errors.append("dashboard: keep the Runners capacity tab")
        if "selcapzone" in dash and "ru-central1-d" not in dash:
            errors.append("dashboard: cap_zone default must be ru-central1-d")
        if "selcapdates" in dash and "__interval___relative_-4d___relative_+0d" not in dash:
            errors.append("dashboard: Capacity Dates default must be last 4 days")

    return errors
