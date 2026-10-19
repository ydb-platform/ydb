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
    """Draft revId after mode=save. Prefer entry.revId / savedId. Never entry.id (object id)."""
    if not isinstance(payload, dict):
        return None
    entry = payload.get("entry") if isinstance(payload.get("entry"), dict) else payload
    for key in ("revId", "savedId"):
        value = entry.get(key)
        if value:
            return value
    return payload.get("revId") or payload.get("savedId")


def _with_rev_id(entry, rev_id):
    if not rev_id:
        return entry
    out = dict(entry)
    out["revId"] = rev_id
    return out


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
    body = {"mode": mode, "entry": _with_rev_id(entry, rev_id)}
    if workbook_id:
        body["workbookId"] = workbook_id
    return rpc("updateEditorChart", body, token, org_id)


def publish_editor_chart(token, org_id, entry, workbook_id=None):
    """Save creates a draft. Publish must put that draft revId on the entry."""
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
    body = {"mode": mode, "entry": _with_rev_id(entry, rev_id)}
    if workbook_id:
        body["workbookId"] = workbook_id
    if dashboard_id:
        body["dashboardId"] = dashboard_id
    return rpc("updateDashboard", body, token, org_id)


def validate_dataset(token, org_id, dataset, workbook_id, dataset_id):
    """Fill result_schema from the SQL source. datasetId is required."""
    return rpc(
        "validateDataset",
        {"datasetId": dataset_id, "workbookId": workbook_id, "data": {"dataset": dataset}},
        token,
        org_id,
    )


def refresh_dataset_fields(token, org_id, dataset, workbook_id, dataset_id):
    """Re-read the SQL source and rebuild fields. A plain validate leaves raw_schema empty."""
    sources = dataset.get("sources") or []
    if not sources or not sources[0].get("id"):
        raise DataLensError("validateDataset", 0, "dataset has no source id to refresh")
    return rpc(
        "validateDataset",
        {
            "datasetId": dataset_id,
            "workbookId": workbook_id,
            "data": {
                "dataset": dataset,
                "updates": [
                    {
                        "action": "refresh_source",
                        "order_index": 0,
                        "source": {"id": sources[0]["id"], "force_update_fields": True},
                    }
                ],
            },
        },
        token,
        org_id,
    )


def dataset_from_validate(payload):
    if not isinstance(payload, dict):
        return None
    if isinstance(payload.get("dataset"), dict):
        return payload["dataset"]
    data = payload.get("data")
    if isinstance(data, dict) and isinstance(data.get("dataset"), dict):
        return data["dataset"]
    return None


def create_dataset(token, org_id, name, dataset, workbook_id):
    """createDataset. Public docs use workbook_id; retry workbookId on HTTP 400."""
    body = {"name": name, "dataset": dataset, "workbook_id": workbook_id}
    try:
        return rpc("createDataset", body, token, org_id)
    except DataLensError as exc:
        if exc.status != 400:
            raise
        return rpc(
            "createDataset",
            {"name": name, "dataset": dataset, "workbookId": workbook_id},
            token,
            org_id,
        )


def dataset_id_from(payload):
    if not isinstance(payload, dict):
        return None
    for key in ("id", "datasetId", "entryId"):
        if payload.get(key):
            return payload[key]
    dataset = payload.get("dataset") if isinstance(payload.get("dataset"), dict) else {}
    return dataset.get("id") or dataset.get("dataset_id")


def create_editor_chart(token, org_id, entry, mode="publish"):
    """createEditorChart. mode is save or publish."""
    return rpc("createEditorChart", {"mode": mode, "entry": entry}, token, org_id)


def entry_id_from(payload):
    if not isinstance(payload, dict):
        return None
    entry = payload.get("entry") if isinstance(payload.get("entry"), dict) else payload
    return entry.get("entryId") or entry.get("id")


def publish_dashboard(token, org_id, entry, workbook_id=None, dashboard_id=None):
    """Save creates a draft. Publish that revision by putting revId on the entry.

    updateDashboard rejects a root-level revId.
    """
    saved = update_dashboard(token, org_id, entry, "save", workbook_id=workbook_id, dashboard_id=dashboard_id)
    draft = walk_rev_id(saved)
    if not draft:
        raise DataLensError("updateDashboard", 0, "save returned no revId: %s" % json.dumps(saved)[:400])
    published = update_dashboard(
        token, org_id, entry, "publish", rev_id=draft, workbook_id=workbook_id, dashboard_id=dashboard_id
    )
    return {"save": saved, "publish": published, "revId": draft}
