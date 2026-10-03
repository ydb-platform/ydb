#!/usr/bin/env python3
"""Local invariants for CI metrics DataLens objects. No network."""

from __future__ import annotations

import json
import re

from store import collect_texts


IN_PROGRESS_COALESCE = re.compile(r"COALESCE\s*\([^)]*in_progress", re.I)
RUN_LABEL_DATE = "Unicode::Substring(CAST(s.started_at AS Utf8), 0, 10)"


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

    gantt_js = (texts.get("gantt") or {}).get("prepare") or ""
    if gantt_js:
        for marker in ("parseRunRef", "keepOneAttempt"):
            if marker not in gantt_js:
                errors.append("gantt: prepare.js must define %s" % marker)
        if "run_label" not in gantt_js:
            errors.append("gantt: selector run_label must beat gantt_run when choosing a run")

    duration_js = (texts.get("duration") or {}).get("prepare") or ""
    if duration_js:
        if "ci_run_id" not in duration_js:
            errors.append("duration: prepare.js must read ci_run_id")
        if "run_attempt" not in duration_js:
            errors.append("duration: prepare.js must group points by run_attempt")
        if "run_label: [String(point.runId" in duration_js:
            errors.append("duration: do not write a short run_id · #attempt into run_label (empties the PR selector)")

    dash = (texts.get("dashboard") or {}).get("dashboard") or ""
    if dash:
        defaults = _seljobstatus_default(dash)
        if defaults is not None and list(defaults) != ["success", "failure"]:
            errors.append("dashboard: job status selector default must be success+failure")
        if IN_PROGRESS_COALESCE.search(dash):
            errors.append("dashboard: do not invent in_progress via COALESCE")

    return errors
