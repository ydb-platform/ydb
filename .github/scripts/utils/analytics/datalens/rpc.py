#!/usr/bin/env python3
"""DataLens API v2 RPC. Token from DATALENS_TOKEN or `yc iam create-token`."""

from __future__ import annotations

import json
import os
import subprocess
import urllib.error
import urllib.request
from typing import Any

API_ROOT = "https://api.datalens.tech/rpc"
API_VERSION = "2"


class DataLensError(RuntimeError):
    def __init__(self, method, status, body):
        self.method = method
        self.status = status
        self.body = body
        RuntimeError.__init__(self, "DataLens %s failed (%s): %s" % (method, status, body[:500]))


def iam_token(explicit=None):
    if explicit:
        return explicit.strip()
    env = os.environ.get("DATALENS_TOKEN") or os.environ.get("YC_TOKEN")
    if env:
        return env.strip()
    try:
        return subprocess.check_output(["yc", "iam", "create-token"], text=True).strip()
    except OSError as exc:
        raise DataLensError("auth", 0, "yc not found: %s" % exc)
    except subprocess.CalledProcessError as exc:
        raise DataLensError("auth", exc.returncode, "yc iam create-token failed")


def rpc(method, body, token, org_id, timeout=120):
    req = urllib.request.Request(
        "%s/%s" % (API_ROOT, method),
        data=json.dumps(body).encode("utf-8"),
        headers={
            "accept": "application/json",
            "content-type": "application/json",
            "authorization": "Bearer %s" % token,
            "x-dl-api-version": API_VERSION,
            "x-dl-org-id": org_id,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        raise DataLensError(method, exc.code, exc.read().decode("utf-8", "replace"))
    if not raw:
        return {}
    return json.loads(raw)


def walk_rev_id(payload):
    """Draft revId after mode=save. Prefer entry.revId / savedId over publishedId."""
    if not isinstance(payload, dict):
        return None
    entry = payload.get("entry") if isinstance(payload.get("entry"), dict) else payload
    for key in ("revId", "savedId", "id"):
        value = entry.get(key)
        if value:
            return value
    return payload.get("revId") or payload.get("savedId")


def get_dashboard(token, org_id, dashboard_id, workbook_id=None):
    body = {"dashboardId": dashboard_id}
    if workbook_id:
        body["workbookId"] = workbook_id
    return rpc("getDashboard", body, token, org_id)


def get_editor_chart(token, org_id, chart_id, workbook_id=None, rev_id=None):
    body = {"chartId": chart_id}
    if workbook_id:
        body["workbookId"] = workbook_id
    if rev_id:
        body["revId"] = rev_id
    return rpc("getEditorChart", body, token, org_id)


def get_dataset(token, org_id, dataset_id, workbook_id):
    return rpc("getDataset", {"datasetId": dataset_id, "workbookId": workbook_id}, token, org_id)


def update_editor_chart(token, org_id, entry, mode, rev_id=None, workbook_id=None):
    body = {"mode": mode, "entry": entry}
    if rev_id:
        body["revId"] = rev_id
    if workbook_id:
        body["workbookId"] = workbook_id
    return rpc("updateEditorChart", body, token, org_id)


def publish_editor_chart(token, org_id, entry, workbook_id=None):
    """Save creates a draft. Publish must use that draft revId or UI stays on the old JS."""
    saved = update_editor_chart(token, org_id, entry, "save", workbook_id=workbook_id)
    draft = walk_rev_id(saved)
    if not draft:
        raise DataLensError("updateEditorChart", 0, "save returned no revId: %s" % json.dumps(saved)[:400])
    published = update_editor_chart(token, org_id, entry, "publish", rev_id=draft, workbook_id=workbook_id)
    return {"save": saved, "publish": published, "revId": draft}


def update_dataset(token, org_id, dataset_id, workbook_id, dataset):
    return rpc(
        "updateDataset",
        {"datasetId": dataset_id, "workbookId": workbook_id, "data": {"dataset": dataset}},
        token,
        org_id,
    )


def update_dashboard(token, org_id, entry, mode, rev_id=None, workbook_id=None, dashboard_id=None):
    body = {"mode": mode, "entry": entry}
    if rev_id:
        body["revId"] = rev_id
    return rpc("updateDashboard", body, token, org_id)


def publish_dashboard(token, org_id, entry, workbook_id=None, dashboard_id=None):
    """Save creates a draft. Publish must use that draft revId."""
    saved = update_dashboard(token, org_id, entry, "save")
    draft = walk_rev_id(saved)
    if not draft:
        raise DataLensError("updateDashboard", 0, "save returned no revId: %s" % json.dumps(saved)[:400])
    published = update_dashboard(token, org_id, entry, "publish", rev_id=draft)
    return {"save": saved, "publish": published, "revId": draft}
