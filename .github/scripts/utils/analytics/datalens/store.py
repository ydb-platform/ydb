#!/usr/bin/env python3
"""Local layout of CI metrics DataLens objects."""

from __future__ import annotations

import json
import uuid
from pathlib import Path
HERE = Path(__file__).resolve().parent
OBJECTS = HERE / "objects"
MANIFEST_PATH = HERE / "manifest.json"
QUERY_PLACEHOLDER = "$QUERY_SQL"
DATASET_DROP = frozenset(["component_errors"])
OBJECT_DIRS = {
    "dashboard": OBJECTS / "dashboard",
    "duration": OBJECTS / "charts" / "duration",
    "gantt": OBJECTS / "charts" / "gantt",
    "pdiff": OBJECTS / "charts" / "pdiff",
    "capacity-tight": OBJECTS / "charts" / "capacity-tight",
    "capacity-use": OBJECTS / "charts" / "capacity-use",
    "capacity-pool": OBJECTS / "charts" / "capacity-pool",
    "capacity-queue": OBJECTS / "charts" / "capacity-queue",
    "duration-ds": OBJECTS / "datasets" / "duration",
    "gantt-ds": OBJECTS / "datasets" / "gantt",
    "pickers-ds": OBJECTS / "datasets" / "pickers",
    "capacity-ds": OBJECTS / "datasets" / "capacity-ds",
}


def load_manifest(path=None):
    with open(path or MANIFEST_PATH, encoding="utf-8") as handle:
        return json.load(handle)


def object_names(manifest, names=None):
    objects = manifest["objects"]
    if not names:
        return list(objects)
    unknown = [name for name in names if name not in objects]
    if unknown:
        raise SystemExit("unknown object(s): %s (have: %s)" % (", ".join(unknown), ", ".join(objects)))
    return list(names)


def object_dir(name, manifest=None):
    if name in OBJECT_DIRS:
        return OBJECT_DIRS[name]
    spec = (manifest or load_manifest())["objects"].get(name) or {}
    kind = spec.get("kind")
    if kind == "chart":
        return OBJECTS / "charts" / name
    if kind == "dataset":
        return OBJECTS / "datasets" / name
    if kind == "dashboard":
        return OBJECTS / "dashboard"
    raise KeyError(name)


def save_manifest(manifest, path=None):
    write_json(path or MANIFEST_PATH, manifest)


def ydb_connection_id(dataset_name="duration-ds"):
    dataset = read_json(object_dir(dataset_name) / "dataset.json")
    for source in dataset.get("sources") or []:
        if source.get("connection_id"):
            return source["connection_id"]
    raise SystemExit("%s has no connection_id" % dataset_name)


def ydb_subselect_dataset(title, sql, connection_id, description=""):
    """Minimal YDB subquery dataset. validateDataset fills raw_schema and result_schema."""
    source_id = str(uuid.uuid4())
    avatar_id = str(uuid.uuid4())
    return {
        "sources": [
            {
                "id": source_id,
                "title": title,
                "connection_id": connection_id,
                "source_type": "YDB_SUBSELECT",
                "managed_by": "user",
                "valid": True,
                "virtual": False,
                "parameters": {"subsql": sql, "manual": True},
                "raw_schema": [],
                "index_info_set": None,
            }
        ],
        "source_avatars": [
            {
                "id": avatar_id,
                "source_id": source_id,
                "title": title,
                "is_root": True,
                "managed_by": "user",
                "valid": True,
                "virtual": False,
            }
        ],
        "avatar_relations": [],
        "result_schema": [],
        "result_schema_aux": {"inter_dependencies": {"deps": []}},
        "obligatory_filters": [],
        "rls": {},
        "rls2": {},
        "template_enabled": False,
        "preview_enabled": True,
        "load_preview_by_default": True,
        "data_export_forbidden": False,
        "description": description,
    }


def chart_create_entry(name, title, workbook_id, dataset_id):
    entry = chart_entry(name, "")
    entry.pop("entryId", None)
    entry["workbookId"] = workbook_id
    entry["name"] = title
    if not entry.get("annotation"):
        entry["annotation"] = {"description": title}
    raw = (entry.get("data") or {}).get("meta") or ""
    try:
        parsed = json.loads(raw) if str(raw).strip() else {}
    except ValueError:
        parsed = {}
    if not isinstance(parsed, dict):
        parsed = {}
    links = parsed.get("links") if isinstance(parsed.get("links"), dict) else {}
    links["dataset"] = dataset_id
    parsed["links"] = links
    entry["data"]["meta"] = json.dumps(parsed, indent=4) + "\n"
    return entry


def chart_widget(widget_id, title, chart_id):
    return {
        "id": widget_id,
        "data": {
            "tabs": [
                {
                    "id": "t" + widget_id,
                    "title": title,
                    "params": {},
                    "chartId": chart_id,
                    "isDefault": True,
                    "autoHeight": False,
                    "enableActionParams": False,
                }
            ],
            "hideTitle": False,
        },
        "type": "widget",
        "namespace": "default",
    }


def dashboard_tab(tab_id, title, widgets):
    items = []
    layout = []
    y = 0
    for spec in widgets:
        items.append(chart_widget(spec["widget_id"], spec["title"], spec["chart_id"]))
        height = int(spec["height"])
        layout.append({"h": height, "i": spec["widget_id"], "w": 36, "x": 0, "y": y})
        y += height
    return {
        "id": tab_id,
        "title": title,
        "items": items,
        "layout": layout,
        "aliases": {"default": []},
        "connections": [],
    }


def upsert_dashboard_tab(entry, tab):
    data = entry.setdefault("data", {})
    tabs = [item for item in (data.get("tabs") or []) if item.get("id") != tab["id"]]
    tabs.append(tab)
    data["tabs"] = tabs
    settings = data.setdefault("settings", {})
    settings["hideTabs"] = False
    return entry


def write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")


def normalize_text(text):
    """Same trailing newline as write_text, so pull/diff do not false-positive."""
    if text is None:
        return ""
    text = str(text)
    if text and not text.endswith("\n"):
        text += "\n"
    return text


def write_text(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(normalize_text(text), encoding="utf-8")


def read_json(path):
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def dump_preview(payload, limit=400):
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True) if not isinstance(payload, str) else payload
    return text if len(text) <= limit else text[:limit] + "…"


def _entry(payload):
    if isinstance(payload, dict) and isinstance(payload.get("entry"), dict):
        return payload["entry"]
    return payload


def extract_chart(payload):
    entry = _entry(payload)
    data = entry.get("data") or {}
    return {
        "sources": data.get("sources") or "",
        "prepare": data.get("prepare") or "",
        "meta": {
            "type": entry.get("type") or "advanced-chart_node",
            "annotation": entry.get("annotation") or {},
            "meta": entry.get("meta") or {},
            "data": {
                "meta": data.get("meta") or "",
                "params": data.get("params") or "",
                "controls": data.get("controls") or "",
            },
        },
    }


def write_chart(name, payload):
    extracted = extract_chart(payload)
    dest = object_dir(name)
    write_text(dest / "sources.js", extracted["sources"])
    write_text(dest / "prepare.js", extracted["prepare"])
    write_json(dest / "meta.json", extracted["meta"])
    return dest


def read_chart(name):
    dest = object_dir(name)
    meta = read_json(dest / "meta.json")
    return {
        "sources": (dest / "sources.js").read_text(encoding="utf-8"),
        "prepare": (dest / "prepare.js").read_text(encoding="utf-8"),
        "meta": meta,
    }


def chart_entry(name, chart_id):
    local = read_chart(name)
    meta = local["meta"]
    data = dict(meta.get("data") or {})
    data["sources"] = local["sources"]
    data["prepare"] = local["prepare"]
    return {
        "entryId": chart_id,
        "type": meta.get("type") or "advanced-chart_node",
        "meta": meta.get("meta") or {},
        "links": {},
        "annotation": meta.get("annotation") or {},
        "data": data,
    }


def dataset_sql(payload):
    dataset = payload.get("dataset") or payload
    for source in dataset.get("sources") or []:
        sql = (source.get("parameters") or {}).get("subsql")
        if sql:
            return sql
    return ""


def slim_dataset(payload):
    dataset = dict(payload.get("dataset") or payload)
    for key in DATASET_DROP:
        dataset.pop(key, None)
    for source in dataset.get("sources") or []:
        params = source.get("parameters")
        if isinstance(params, dict) and "subsql" in params:
            params = dict(params)
            params["subsql"] = QUERY_PLACEHOLDER
            source["parameters"] = params
    return dataset


def inject_sql(dataset, sql):
    out = json.loads(json.dumps(dataset))
    for source in out.get("sources") or []:
        params = source.get("parameters")
        if isinstance(params, dict) and params.get("subsql") == QUERY_PLACEHOLDER:
            params["subsql"] = sql
    return out


def write_dataset(name, payload):
    dest = object_dir(name)
    write_text(dest / "query.sql", dataset_sql(payload))
    write_json(dest / "dataset.json", slim_dataset(payload))
    return dest


def read_dataset(name):
    dest = object_dir(name)
    return {
        "sql": (dest / "query.sql").read_text(encoding="utf-8"),
        "dataset": inject_sql(read_json(dest / "dataset.json"), (dest / "query.sql").read_text(encoding="utf-8")),
    }


def extract_dashboard(payload):
    entry = _entry(payload)
    return {
        "entryId": entry.get("entryId") or entry.get("id"),
        "type": entry.get("type") or "",
        "annotation": entry.get("annotation") or {},
        "meta": entry.get("meta") or {},
        "data": entry.get("data") or {},
    }


def write_dashboard(name, payload):
    dest = object_dir(name)
    write_json(dest / "entry.json", extract_dashboard(payload))
    return dest


def read_dashboard(name):
    return read_json(object_dir(name) / "entry.json")


def dashboard_entry(name, dashboard_id):
    local = read_dashboard(name)
    return {
        "entryId": local.get("entryId") or dashboard_id,
        "meta": local.get("meta") or {},
        "annotation": local.get("annotation") or {},
        "data": local.get("data") or {},
    }


def write_live(name, kind, payload):
    if kind == "chart":
        return write_chart(name, payload)
    if kind == "dataset":
        return write_dataset(name, payload)
    if kind == "dashboard":
        return write_dashboard(name, payload)
    raise ValueError("unknown kind %s" % kind)


def local_fingerprint(name, kind):
    if kind == "chart":
        local = read_chart(name)
        return {
            "sources": normalize_text(local["sources"]),
            "prepare": normalize_text(local["prepare"]),
            "params": (local["meta"].get("data") or {}).get("params") or "",
            "controls": (local["meta"].get("data") or {}).get("controls") or "",
        }
    if kind == "dataset":
        local = read_dataset(name)
        schema = [field.get("guid") or field.get("title") or field.get("name") for field in (local["dataset"].get("result_schema") or [])]
        return {"sql": normalize_text(local["sql"]), "result_schema": schema}
    if kind == "dashboard":
        data = read_dashboard(name).get("data") or {}
        return {"tabs": data.get("tabs"), "settings": data.get("settings")}
    raise ValueError("unknown kind %s" % kind)


def live_fingerprint(kind, payload):
    if kind == "chart":
        extracted = extract_chart(payload)
        return {
            "sources": normalize_text(extracted["sources"]),
            "prepare": normalize_text(extracted["prepare"]),
            "params": extracted["meta"]["data"]["params"],
            "controls": extracted["meta"]["data"]["controls"],
        }
    if kind == "dataset":
        dataset = payload.get("dataset") or payload
        schema = [field.get("guid") or field.get("title") or field.get("name") for field in (dataset.get("result_schema") or [])]
        return {"sql": normalize_text(dataset_sql(payload)), "result_schema": schema}
    if kind == "dashboard":
        data = extract_dashboard(payload).get("data") or {}
        return {"tabs": data.get("tabs"), "settings": data.get("settings")}
    raise ValueError("unknown kind %s" % kind)


def collect_texts():
    """name -> {sql, js, dashboard} strings for local invariant checks."""
    out = {}
    for name, dest in OBJECT_DIRS.items():
        texts = {}
        sql = dest / "query.sql"
        if sql.is_file():
            texts["sql"] = sql.read_text(encoding="utf-8")
        prepare = dest / "prepare.js"
        if prepare.is_file():
            texts["prepare"] = prepare.read_text(encoding="utf-8")
        sources = dest / "sources.js"
        if sources.is_file():
            texts["sources"] = sources.read_text(encoding="utf-8")
        entry = dest / "entry.json"
        if entry.is_file():
            texts["dashboard"] = entry.read_text(encoding="utf-8")
        if texts:
            out[name] = texts
    return out
