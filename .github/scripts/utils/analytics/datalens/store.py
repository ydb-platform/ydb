#!/usr/bin/env python3
"""Local layout of pulled DataLens objects."""

from __future__ import annotations

import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
OBJECTS = HERE / "objects"
MANIFEST_PATH = HERE / "manifest.json"
QUERY_PLACEHOLDER = "$QUERY_SQL"
DATASET_DROP = frozenset(["component_errors"])


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
    spec = (manifest or load_manifest())["objects"].get(name) or {}
    kind = spec.get("kind")
    if kind == "chart":
        return OBJECTS / "charts" / name
    if kind == "dataset":
        return OBJECTS / "datasets" / name
    if kind == "dashboard":
        return OBJECTS / "dashboard"
    raise KeyError(name)


def write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")


def normalize_text(text):
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
    return {
        "sources": (dest / "sources.js").read_text(encoding="utf-8"),
        "prepare": (dest / "prepare.js").read_text(encoding="utf-8"),
        "meta": read_json(dest / "meta.json"),
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
    sql = (dest / "query.sql").read_text(encoding="utf-8")
    return {"sql": sql, "dataset": inject_sql(read_json(dest / "dataset.json"), sql)}


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
