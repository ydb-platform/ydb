#!/usr/bin/env python3
"""Snapshot Cloud quota and the ghrun pool into analytics/ci_capacity."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

try:
    import jwt
except ImportError:
    jwt = None

_ANALYTICS_ROOT = Path(__file__).resolve().parents[1]
if str(_ANALYTICS_ROOT) not in sys.path:
    sys.path.insert(0, str(_ANALYTICS_ROOT))

from collector.flush import has_send_credentials
from collector.schema import TTL_MINUTES, _open_ydb_wrapper, build_column_types, resolve_table_path
from collector.values import parse_datetime
from github_actions.export_github_job_metrics import attach_pull_requests, list_run_jobs
from github_actions.github_api import github_get

DEFAULT_TABLE_PATH = "analytics/ci_capacity"
TABLE_CONFIG_KEY = "ci_capacity"
DEFAULT_CLOUD_ID = "b1ggceeul2pkher8vhb6"
DEFAULT_ORG = "ydb-platform"
DEFAULT_REPO = "ydb"
GIB = 1024.0 ** 3
BYTES_THRESHOLD = 1_000_000.0
QUOTA_FIELDS = (
    ("compute.instanceCores.count", "cores", False),
    ("compute.instanceMemory.size", "ram_gib", True),
    ("compute.instances.count", "instances", False),
    ("compute.ssdNonReplicatedDisks.size", "nrd_gib", True),
)
FIT_PRESETS = {
    "relwithdebinfo": "build-preset-relwithdebinfo",
    "release-asan": "build-preset-release-asan",
}
JOB_RANK = {"in_progress": 2, "queued": 1}
GITHUB_HOSTED_PREFIXES = ("ubuntu-", "windows-", "macos-")
GHRUN_NAME_PREFIXES = ("ghrun-", "static-", "tiny-worker-", "analytic-node-")
PR_EVENTS = ("pull_request", "pull_request_target")
RUN_PAGE_SIZE = 100
RUN_MAX_PAGES = 30
QUOTA_URL = "https://quota-manager.api.cloud.yandex.net/quota-manager/v1/quotaLimits"
FOLDERS_URL = "https://resource-manager.api.cloud.yandex.net/resource-manager/v1/folders"
INSTANCES_URL = "https://compute.api.cloud.yandex.net/compute/v1/instances"
SIZE_KEYS = ("vcpu", "ram_gb", "nrd_ssd_gb")
OPTIONAL = (
    "workflow", "job_name", "branch", "run_url", "pr_number", "pr_title",
    "state", "zone", "cores", "ram_gib", "nrd_gib",
    "limit_value", "usage_value", "free_value", "created_at", "age_sec",
)

COLUMNS = (
    ("sampled_at", "Timestamp", False),
    ("date", "Date", False),
    ("kind", "Utf8", False),
    ("name", "Utf8", False),
) + tuple((name, "Timestamp" if name == "created_at" else "Double" if name.endswith(("_gib", "_value", "_sec")) or name == "cores" else "Utf8", True) for name in OPTIONAL)
PRIMARY_KEYS = ("sampled_at", "date", "kind", "name")


class IncompleteGitHubJobs(RuntimeError):
    pass


def repo_root() -> Path:
    return Path(__file__).resolve().parents[5]


def footprints_path() -> Path:
    return repo_root() / ".github" / "config" / "runners_footprints.yml"


def resolve_capacity_table_path(ydb_wrapper=None) -> str:
    return resolve_table_path(ydb_wrapper, table_config_key=TABLE_CONFIG_KEY, default=DEFAULT_TABLE_PATH)


def build_create_capacity_table_sql(table_path: str, ttl_minutes: int = TTL_MINUTES) -> str:
    cols = ",\n".join(
        "            `%s` %s%s" % (name, sql_type, "" if nullable else " NOT NULL")
        for name, sql_type, nullable in COLUMNS
    )
    pk = ", ".join("`%s`" % key for key in PRIMARY_KEYS)
    return f"""
        CREATE TABLE IF NOT EXISTS `{table_path}` (
{cols},
            PRIMARY KEY ({pk})
        )
        PARTITION BY HASH(`date`)
        WITH (
            STORE = COLUMN,
            AUTO_PARTITIONING_BY_SIZE = ENABLED,
            AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048,
            AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4,
            TTL = Interval("PT{int(ttl_minutes)}M") ON `sampled_at`
        )
    """


def as_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def bytes_to_gib(value: float) -> float:
    return value / GIB if value >= BYTES_THRESHOLD else value


def _pick(mapping: Any, *keys: str):
    if not isinstance(mapping, dict):
        return None
    for key in keys:
        value = mapping.get(key)
        if value not in (None, ""):
            return value
    return None


def _row(sampled_at: datetime, kind: str, name: str, **fields: Any) -> Dict[str, Any]:
    row = dict.fromkeys(OPTIONAL)
    row.update(sampled_at=sampled_at, date=sampled_at.date(), kind=kind, name=name, **fields)
    return row


def _size(raw: Any, fallback: Optional[Dict[str, float]] = None) -> Dict[str, float]:
    fallback = fallback or {}
    out: Dict[str, float] = {}
    for key in SIZE_KEYS:
        number = as_float((raw or {}).get(key))
        if number is None:
            number = as_float(fallback.get(key))
        if number is not None:
            out[key] = number
    return out


def load_label_footprints(path: Optional[Path] = None):
    path = path or footprints_path()
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {} if path.is_file() else {}
    except (OSError, yaml.YAMLError):
        data = {}
    if not isinstance(data, dict):
        data = {}
    default = _size(data.get("default_footprint"))
    specs: Dict[str, Dict[str, float]] = {}
    for label, raw in (data.get("footprints") or {}).items():
        if isinstance(raw, dict):
            specs[str(label)] = _size(raw, default)
    for label, raw in (data.get("static_runners") or {}).items():
        if isinstance(raw, dict):
            spec = _size(raw.get("footprint") or raw, default)
            if spec:
                specs[str(label)] = spec
    return specs, default


def load_fit_footprints(path: Optional[Path] = None, specs=None, default=None) -> Dict[str, Dict[str, float]]:
    if specs is None:
        specs, default = load_label_footprints(path)
    return {
        short: dict(specs.get(label) or default)
        for short, label in FIT_PRESETS.items()
        if specs.get(label) or default
    }


def _quota_az(quota_id: str, field: str) -> Optional[str]:
    if quota_id == field:
        return ""
    prefix = field + "."
    return quota_id[len(prefix):] if quota_id.startswith(prefix) else None


def _add_quota(bucket: Dict[str, Dict[str, float]], name: str, limit: float, usage: float) -> None:
    spec = bucket.setdefault(name, {"limit": 0.0, "usage": 0.0, "free": 0.0})
    spec["limit"] += limit
    spec["usage"] += usage
    spec["free"] = spec["limit"] - spec["usage"]


def _quotas(limits: List[Dict[str, Any]]):
    cloud: Dict[str, Dict[str, float]] = {}
    zonal: Dict[str, Dict[str, Dict[str, float]]] = {}
    for row in limits:
        if not isinstance(row, dict):
            continue
        quota_id = row.get("quota_id") or row.get("quotaId") or ""
        limit, usage = as_float(row.get("limit")), as_float(row.get("usage"))
        if limit is None or usage is None:
            continue
        for field, name, as_bytes in QUOTA_FIELDS:
            az = _quota_az(quota_id, field)
            if az is None:
                continue
            if as_bytes:
                limit_v, usage_v = limit / GIB, usage / GIB
            else:
                limit_v, usage_v = limit, usage
            _add_quota(cloud, name, limit_v, usage_v)
            if az:
                _add_quota(zonal.setdefault(az, {}), name, limit_v, usage_v)
    return cloud, zonal


def quota_headroom(limits: List[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    return _quotas(limits)[0]


def quota_headroom_by_zone(limits: List[Dict[str, Any]]) -> Dict[str, Dict[str, Dict[str, float]]]:
    return _quotas(limits)[1]


def hosts_that_fit(headroom: Dict[str, Dict[str, float]], footprint: Dict[str, float]) -> int:
    bounds = []
    for key, size_key in (("cores", "vcpu"), ("ram_gib", "ram_gb"), ("nrd_gib", "nrd_ssd_gb")):
        slot, size = headroom.get(key), footprint.get(size_key)
        if slot is not None and size:
            bounds.append(int(slot["free"] // size))
    if "instances" in headroom:
        bounds.append(int(headroom["instances"]["free"]))
    return max(min(bounds), 0) if bounds else 0


def is_ghrun(instance: Dict[str, Any]) -> bool:
    labels = instance.get("labels") or {}
    return (isinstance(labels, dict) and "ghrun" in labels) or (instance.get("name") or "").startswith(GHRUN_NAME_PREFIXES)


def instance_zone(instance: Dict[str, Any]) -> str:
    return str(_pick(instance, "zoneId", "zone_id", "zone") or "").strip()


def instance_cores(instance: Dict[str, Any]) -> Optional[float]:
    return as_float(_pick(instance.get("resources") or {}, "cores") or instance.get("cores"))


def instance_ram_gib(instance: Dict[str, Any]) -> Optional[float]:
    raw = as_float(_pick(instance.get("resources") or {}, "memory") or instance.get("ram"))
    return None if raw is None else bytes_to_gib(raw)


def instance_nrd_gib(instance: Dict[str, Any], specs=None, default=None) -> Optional[float]:
    disks = []
    boot = _pick(instance, "boot_disk", "bootDisk")
    if isinstance(boot, dict):
        disks.append(boot)
    extra = _pick(instance, "secondary_disks", "secondaryDisks") or []
    if isinstance(extra, list):
        disks.extend(item for item in extra if isinstance(item, dict))
    total, found = 0.0, False
    for disk in disks:
        spec = _pick(disk, "disk_spec", "diskSpec") or disk
        size = as_float(_pick(spec, "size", "disk_size") or disk.get("size"))
        dtype = str(_pick(spec, "type_id", "typeId", "type") or disk.get("type") or "").lower()
        if size is not None and ("nonreplicated" in dtype or "nrd" in dtype):
            total += bytes_to_gib(size)
            found = True
    if found:
        return total
    if not is_ghrun(instance):
        return None
    if specs is None:
        specs, default = load_label_footprints()
    name = instance.get("name") or ""
    if name.startswith("tiny-worker-"):
        spec = specs.get("tiny-worker") or {}
    elif name.startswith("analytic-node-"):
        spec = specs.get("build-preset-analytic-node") or {}
    elif (instance_ram_gib(instance) or 0) <= 16 and (instance_cores(instance) or 0) <= 8:
        spec = specs.get("tiny-worker") or specs.get("build-preset-analytic-node") or {}
    else:
        spec = default or specs.get("build-preset-relwithdebinfo") or {}
    return spec.get("nrd_ssd_gb")


def age_sec(now: datetime, moment: Optional[datetime]) -> Optional[float]:
    return None if moment is None else max((now - moment).total_seconds(), 0.0)


def pull_info(run: Dict[str, Any]) -> tuple:
    run = run or {}
    if run.get("event") not in PR_EVENTS:
        return None, None
    pull = (run.get("pull_requests") or [None])[0]
    if not isinstance(pull, dict) or pull.get("number") is None:
        return None, None
    return str(pull["number"]), str(pull.get("title") or run.get("display_title") or "") or None


def target_branch(run: Dict[str, Any]) -> str:
    pull = ((run or {}).get("pull_requests") or [None])[0]
    if isinstance(pull, dict):
        ref = ((pull.get("base") or {}).get("ref") if isinstance(pull.get("base"), dict) else None)
        if ref:
            return str(ref)
    if (run or {}).get("event") in PR_EVENTS:
        return ""
    return str((run or {}).get("head_branch") or "")


def display_workflow(run: Dict[str, Any], job: Optional[Dict[str, Any]] = None) -> str:
    if str((job or {}).get("name") or "").startswith("Postcommit"):
        return "Postcommit"
    return str((run or {}).get("name") or "") or "(no workflow)"


def index_runner_jobs(pairs: List[tuple]) -> Dict[str, Dict[str, Any]]:
    best: Dict[str, Dict[str, Any]] = {}
    for run, job in pairs:
        if not isinstance(job, dict):
            continue
        runner, status, job_name = job.get("runner_name") or "", job.get("status") or "", job.get("name") or ""
        if not runner or status not in JOB_RANK or "${{" in job_name:
            continue
        current = best.get(runner)
        if current is not None and JOB_RANK[status] <= JOB_RANK[current["status"]]:
            continue
        number, title = pull_info(run or {})
        best[runner] = {
            "status": status,
            "workflow": display_workflow(run or {}, job),
            "job_name": job_name,
            "branch": target_branch(run or {}),
            "run_url": job.get("html_url") or (run or {}).get("html_url") or "",
            "pr_number": number,
            "pr_title": title,
            "created_at": parse_datetime(job.get("created_at")),
        }
    return best


def job_label_names(job: Dict[str, Any]) -> List[str]:
    names = []
    for label in job.get("labels") or []:
        text = str(label.get("name") if isinstance(label, dict) else label).strip()
        if text:
            names.append(text)
    return names


def is_github_hosted_job(job: Dict[str, Any]) -> bool:
    labels = [name.lower() for name in job_label_names(job)]
    if "self-hosted" in labels or any(name.startswith("build-preset-") for name in labels):
        return False
    return any(name.startswith(GITHUB_HOSTED_PREFIXES) for name in labels)


def job_footprint(job: Dict[str, Any], specs: Dict[str, Dict[str, float]], default: Dict[str, float]) -> Dict[str, float]:
    labels = job_label_names(job)
    for label in labels:
        if label in specs:
            return specs[label]
        if "build-preset-" + label in specs:
            return specs["build-preset-" + label]
    blob = ((job.get("name") or "") + " " + " ".join(labels)).lower()
    if any(token in blob for token in ("asan", "msan", "tsan")):
        return specs.get("build-preset-release-asan", default)
    if "relwithdebinfo" in blob or "release" in blob:
        return specs.get("build-preset-relwithdebinfo", default)
    return default


def queue_jobs(pairs: List[tuple], now: datetime, specs=None, default=None) -> List[Dict[str, Any]]:
    if specs is None or default is None:
        loaded_specs, loaded_default = load_label_footprints()
        specs = loaded_specs if specs is None else specs
        default = loaded_default if default is None else default
    jobs, used = [], set()
    for run, job in pairs:
        if not isinstance(job, dict) or job.get("status") != "queued" or is_github_hosted_job(job):
            continue
        job_name = job.get("name") or ""
        if "${{" in job_name:
            continue
        run = run or {}
        created = parse_datetime(job.get("created_at") or run.get("created_at"))
        size = job_footprint(job, specs, default)
        number, title = pull_info(run)
        name = "q.%s.%s" % (run.get("id") or run.get("name") or "wf", job.get("id") or job_name or "job")
        if name in used:
            name = "%s.%s" % (name, len(used))
        used.add(name)
        jobs.append({
            "name": name,
            "workflow": display_workflow(run, job),
            "job_name": job_name,
            "branch": target_branch(run) or None,
            "run_url": job.get("html_url") or run.get("html_url") or None,
            "pr_number": number,
            "pr_title": title,
            "cores": size.get("vcpu") or 0.0,
            "ram": size.get("ram_gb") or 0.0,
            "nrd": size.get("nrd_ssd_gb") or 0.0,
            "created_at": created,
            "age": age_sec(now, created),
        })
    jobs.sort(key=lambda item: (-(item["age"] or 0), item["workflow"], item["job_name"]))
    return jobs


def count_assigned_and_queued(pairs: Optional[List[tuple]]) -> tuple:
    busy, queued = set(), 0
    for _run, job in pairs or []:
        if not isinstance(job, dict):
            continue
        if job.get("status") == "in_progress" and job.get("runner_name"):
            busy.add(job["runner_name"])
        elif job.get("status") == "queued" and not is_github_hosted_job(job):
            queued += 1
    return len(busy), queued


def github_jobs_incomplete(ghrun_n: int, busy_n: int, queued_n: int) -> bool:
    if ghrun_n < 10:
        return False
    return (queued_n >= 20 and busy_n < ghrun_n * 0.35) or (ghrun_n >= 40 and busy_n < ghrun_n * 0.15)


def runner_state(name: str, jobs: Optional[Dict[str, Dict[str, Any]]], github_ok: bool) -> str:
    if not github_ok or jobs is None:
        return "unknown"
    return {"in_progress": "busy", "queued": "queued"}.get((jobs.get(name) or {}).get("status"), "idle")


def unique_name(instance: Dict[str, Any], used: set) -> str:
    name = instance.get("name") or instance.get("id") or "unnamed"
    alt, n = name, 1
    while alt in used:
        n += 1
        folder = instance.get("_folder_id") or "folder"
        alt = "%s@%s" % (name, folder) if n == 2 else "%s@%s.%s" % (name, folder, n)
    used.add(alt)
    return alt


def quota_and_fit_rows(sampled_at, headroom, footprints, zone: str) -> List[Dict[str, Any]]:
    suffix = "@%s" % zone if zone else ""
    rows = []
    for name, spec in headroom.items():
        rows.append(_row(
            sampled_at, "quota", name + suffix, zone=zone or None,
            limit_value=spec["limit"], usage_value=spec["usage"], free_value=spec["free"],
            cores=spec["free"] if name == "cores" else None,
            ram_gib=spec["free"] if name == "ram_gib" else None,
            nrd_gib=spec["free"] if name == "nrd_gib" else None,
        ))
    for label, footprint in footprints.items():
        rows.append(_row(
            sampled_at, "fit", "fit.%s%s" % (label, suffix), zone=zone or None,
            free_value=float(hosts_that_fit(headroom, footprint)),
            cores=footprint.get("vcpu"), ram_gib=footprint.get("ram_gb"), nrd_gib=footprint.get("nrd_ssd_gb"),
        ))
    return rows


def append_runner_summary(rows, sampled_at, ghruns, zone: str) -> None:
    suffix = "@%s" % zone if zone else ""
    created = [item for item in ghruns if item["created_at"] is not None]
    newest = max(created, key=lambda item: item["created_at"]) if created else None
    born = [item for item in created if sampled_at - item["created_at"] <= timedelta(hours=24)]
    idle = [item for item in ghruns if item["state"] == "idle"]
    busy = [item for item in ghruns if item["state"] == "busy"]
    zone_v = zone or None
    rows.append(_row(
        sampled_at, "summary", "runners.ghrun" + suffix, zone=zone_v,
        usage_value=float(len(ghruns)), free_value=float(len(idle)),
        cores=sum(item["cores"] or 0 for item in busy),
        ram_gib=sum(item["ram"] or 0 for item in busy),
        nrd_gib=sum(item["nrd"] or 0 for item in busy),
        limit_value=sum(item["ram"] or 0 for item in ghruns),
    ))
    rows.append(_row(
        sampled_at, "summary", "runners.idle" + suffix, zone=zone_v,
        usage_value=float(len(idle)),
        cores=sum(item["cores"] or 0 for item in idle),
        ram_gib=sum(item["ram"] or 0 for item in idle),
        nrd_gib=sum(item["nrd"] or 0 for item in idle),
    ))
    rows.append(_row(sampled_at, "summary", "runners.created_24h" + suffix, zone=zone_v, usage_value=float(len(born))))
    if newest:
        rows.append(_row(
            sampled_at, "summary", "runners.newest" + suffix, zone=zone_v,
            job_name=newest["name"], created_at=newest["created_at"],
            age_sec=age_sec(sampled_at, newest["created_at"]), usage_value=float(len(ghruns)),
        ))


def build_snapshot_rows(
    now: datetime,
    limits: List[Dict[str, Any]],
    instances: List[Dict[str, Any]],
    pairs: Optional[List[tuple]],
    footprints: Dict[str, Dict[str, float]],
    github_ok: bool,
    label_specs=None,
    label_default=None,
) -> List[Dict[str, Any]]:
    sampled_at = now.replace(microsecond=0)
    if label_specs is None or label_default is None:
        loaded_specs, loaded_default = load_label_footprints()
        label_specs = loaded_specs if label_specs is None else label_specs
        label_default = loaded_default if label_default is None else label_default
    cloud, zonal = _quotas(limits)
    rows = quota_and_fit_rows(sampled_at, cloud, footprints, "")
    for az in sorted(zonal):
        rows.extend(quota_and_fit_rows(sampled_at, zonal[az], footprints, az))

    jobs = index_runner_jobs(pairs) if github_ok and pairs is not None else None
    used, ghruns = set(), []
    for instance in instances:
        if not isinstance(instance, dict) or instance.get("status") not in (None, "RUNNING"):
            continue
        name = unique_name(instance, used)
        cores, ram, nrd = instance_cores(instance), instance_ram_gib(instance), instance_nrd_gib(instance, label_specs, label_default)
        created, zone = parse_datetime(_pick(instance, "created_at", "createdAt")), instance_zone(instance) or None
        if is_ghrun(instance):
            state = runner_state(name, jobs, github_ok)
            job = (jobs or {}).get(name) or {}
            ghruns.append({"name": name, "created_at": created, "state": state, "cores": cores, "ram": ram, "nrd": nrd, "zone": zone or ""})
            rows.append(_row(
                sampled_at, "runner", name,
                workflow=job.get("workflow"), job_name=job.get("job_name"),
                branch=job.get("branch") or None, run_url=job.get("run_url") or None,
                pr_number=job.get("pr_number") or None, pr_title=job.get("pr_title") or None,
                state=state, zone=zone, cores=cores, ram_gib=ram, nrd_gib=nrd,
                created_at=created, age_sec=age_sec(sampled_at, created),
            ))
        else:
            rows.append(_row(
                sampled_at, "runner", name, state="other", zone=zone,
                job_name=instance.get("_folder_name") or None,
                cores=cores, ram_gib=ram, nrd_gib=nrd,
                created_at=created, age_sec=age_sec(sampled_at, created),
            ))

    append_runner_summary(rows, sampled_at, ghruns, "")
    by_zone: Dict[str, List[Dict[str, Any]]] = {}
    for item in ghruns:
        if item.get("zone"):
            by_zone.setdefault(item["zone"], []).append(item)
    for az in sorted(by_zone):
        append_runner_summary(rows, sampled_at, by_zone[az], az)

    if github_ok and pairs is not None:
        for item in queue_jobs(pairs, sampled_at, label_specs, label_default):
            rows.append(_row(
                sampled_at, "queue", item["name"],
                workflow=item["workflow"], job_name=item["job_name"] or None,
                branch=item["branch"], run_url=item["run_url"],
                pr_number=item["pr_number"], pr_title=item["pr_title"],
                usage_value=1.0, age_sec=item["age"], created_at=item["created_at"],
                cores=item["cores"], ram_gib=item["ram"], nrd_gib=item["nrd"],
            ))
    return rows


def snapshot_summary(rows: List[Dict[str, Any]]) -> str:
    by_key = {(row["kind"], row["name"]): row for row in rows}

    def free(name: str) -> str:
        row = by_key.get(("quota", name))
        return "?" if not row else "%.0f" % row["free_value"]

    fit_rel, fit_asan = by_key.get(("fit", "fit.relwithdebinfo")), by_key.get(("fit", "fit.release-asan"))
    ghrun, newest = by_key.get(("summary", "runners.ghrun")), by_key.get(("summary", "runners.newest"))
    queued_n = sum(row["usage_value"] or 0 for row in rows if row["kind"] == "queue")
    newest_at = newest["created_at"].isoformat() if newest and newest["created_at"] else "?"
    return (
        "ram_free=%sGiB cores_free=%s instances_free=%s nrd_free=%sGiB "
        "fit_relwithdebinfo=%s fit_asan=%s ghrun=%s idle=%s queued_jobs=%.0f newest=%s"
        % (
            free("ram_gib"), free("cores"), free("instances"), free("nrd_gib"),
            "%.0f" % fit_rel["free_value"] if fit_rel else "?",
            "%.0f" % fit_asan["free_value"] if fit_asan else "?",
            "%.0f" % ghrun["usage_value"] if ghrun else "?",
            "%.0f" % ghrun["free_value"] if ghrun else "?",
            queued_n, newest_at,
        )
    )


IAM_TOKEN_URL = "https://iam.api.cloud.yandex.net/iam/v1/tokens"


def iam_token_from_key_file(path: str) -> str:
    key = json.loads(Path(path).read_text())
    if jwt is None:
        raise RuntimeError("PyJWT is missing; install it to mint an IAM token from the CI service-account key")
    now = int(time.time())
    assertion = jwt.encode(
        {
            "iss": key.get("service_account_id") or key.get("user_account_id"),
            "aud": IAM_TOKEN_URL,
            "iat": now,
            "exp": now + 3600,
        },
        key["private_key"],
        algorithm="PS256",
        headers={"kid": key["id"], "typ": "JWT"},
    )
    if isinstance(assertion, bytes):
        assertion = assertion.decode()
    req = urllib.request.Request(
        IAM_TOKEN_URL,
        data=json.dumps({"jwt": assertion}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        body = (exc.read() or b"").decode("utf-8", "replace").strip().splitlines()
        raise RuntimeError(body[-1] if body else "IAM token request failed") from exc
    token = (payload.get("iamToken") or payload.get("iam_token") or "").strip()
    if not token:
        raise RuntimeError("IAM token response has no iamToken")
    return token


def iam_token() -> str:
    key = (
        os.environ.get("CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS")
        or os.environ.get("YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS")
        or ""
    )
    if key and os.path.isfile(key):
        return iam_token_from_key_file(key)
    for name in ("YC_TOKEN", "YC_IAM_TOKEN"):
        token = (os.environ.get(name) or "").strip()
        if token:
            return token
    raise RuntimeError("no Cloud IAM token: set CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS")


def cloud_get(url: str, token: str, params: Dict[str, str]) -> Any:
    query = urllib.parse.urlencode({key: value for key, value in params.items() if value not in (None, "")})
    req = urllib.request.Request(
        "%s?%s" % (url, query) if query else url,
        headers={"Authorization": "Bearer %s" % token},
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        body = (exc.read() or b"").decode("utf-8", "replace").strip().splitlines()
        raise RuntimeError(body[-1] if body else "%s %s" % (exc.code, url)) from exc


def cloud_list(url: str, token: str, params: Dict[str, str], key: str) -> List[Any]:
    items, page = [], None
    snake = "".join("_" + ch.lower() if ch.isupper() else ch for ch in key).lstrip("_")
    while True:
        query = dict(params)
        if page:
            query["pageToken"] = page
        payload = cloud_get(url, token, query)
        if isinstance(payload, list):
            return payload
        if not isinstance(payload, dict):
            return items
        items.extend(payload.get(key) or payload.get(snake) or [])
        page = payload.get("nextPageToken") or payload.get("next_page_token")
        if not page:
            return items


def fetch_quota(cloud_id: str, token: Optional[str] = None) -> List[Dict[str, Any]]:
    token = token or iam_token()
    rows = []
    for row in cloud_list(QUOTA_URL, token, {
        "resource.id": cloud_id,
        "resource.type": "resource-manager.cloud",
        "service": "compute",
        "pageSize": "1000",
    }, "quotaLimits"):
        if isinstance(row, dict):
            rows.append({
                "quota_id": row.get("quota_id") or row.get("quotaId") or "",
                "limit": row.get("limit"),
                "usage": row.get("usage"),
            })
    return rows


def fetch_instances(cloud_id: str, token: Optional[str] = None) -> List[Dict[str, Any]]:
    token = token or iam_token()
    instances = []
    for folder in cloud_list(FOLDERS_URL, token, {"cloudId": cloud_id, "pageSize": "1000"}, "folders"):
        if not isinstance(folder, dict) or folder.get("status") not in (None, "ACTIVE"):
            continue
        for instance in cloud_list(INSTANCES_URL, token, {"folderId": folder["id"], "pageSize": "1000"}, "instances"):
            if isinstance(instance, dict):
                copied = dict(instance)
                copied["_folder_id"] = folder.get("id") or ""
                copied["_folder_name"] = folder.get("name") or ""
                instances.append(copied)
    return instances


def ensure_github_token() -> bool:
    if os.environ.get("GITHUB_TOKEN"):
        return True
    try:
        token = subprocess.check_output(["gh", "auth", "token"], text=True, stderr=subprocess.DEVNULL).strip()
    except (OSError, subprocess.CalledProcessError):
        return False
    if token:
        os.environ["GITHUB_TOKEN"] = token
    return bool(token)


def list_workflow_runs(org, repo, status, per_page=RUN_PAGE_SIZE, max_pages=RUN_MAX_PAGES, fetcher=None):
    get = fetcher or github_get
    url = "https://api.github.com/repos/%s/%s/actions/runs" % (org, repo)
    runs, seen, last_batch, empty_streak = [], set(), [], 0
    for page in range(1, max_pages + 1):
        last_batch = (get(url, params={"status": status, "per_page": per_page, "page": page}).get("workflow_runs") or [])
        if not last_batch:
            empty_streak += 1
            if empty_streak >= 2:
                return runs
            continue
        empty_streak = 0
        for run in last_batch:
            if run.get("id") not in seen:
                seen.add(run.get("id"))
                runs.append(run)
    raise IncompleteGitHubJobs(
        "workflow runs for status=%s hit max_pages=%s after %s runs (last page %s)"
        % (status, max_pages, len(runs), len(last_batch))
    )


def enrich_run_pull(org: str, repo: str, run: Dict[str, Any]) -> Dict[str, Any]:
    if run.get("event") in PR_EVENTS:
        return attach_pull_requests(org, repo, run)
    if run.get("pull_requests"):
        cleaned = dict(run)
        cleaned["pull_requests"] = []
        return cleaned
    return run


def collect_open_jobs(org: str, repo: str) -> List[tuple]:
    pairs = []
    for status in ("in_progress", "queued"):
        for run in list_workflow_runs(org, repo, status):
            if run.get("id") is None:
                continue
            run = enrich_run_pull(org, repo, run)
            pairs.extend((run, job) for job in list_run_jobs(org, repo, int(run["id"])))
    return pairs


def write_rows(rows: List[Dict[str, Any]], table_path: Optional[str] = None) -> int:
    if not rows:
        print("No capacity rows to write", file=sys.stderr)
        return 1
    if not has_send_credentials():
        print("Analytics YDB credentials are missing", file=sys.stderr)
        return 1
    with _open_ydb_wrapper() as wrapper:
        if not wrapper.check_credentials():
            print("Analytics YDB credentials are missing", file=sys.stderr)
            return 1
        path = table_path or resolve_capacity_table_path(wrapper)
        wrapper.create_table(path, build_create_capacity_table_sql(path))
        wrapper.bulk_upsert_batches(path, rows, build_column_types(COLUMNS), batch_size=500)
    print("Wrote %d rows to %s" % (len(rows), path))
    return 0


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Snapshot Cloud quota and ghrun capacity into YDB")
    parser.add_argument("--org", default=os.environ.get("CI_METRICS_ORG", DEFAULT_ORG))
    parser.add_argument("--repo", default=os.environ.get("CI_METRICS_REPO", DEFAULT_REPO))
    parser.add_argument("--cloud-id", default=os.environ.get("YC_CLOUD_ID", DEFAULT_CLOUD_ID))
    parser.add_argument("--table-path", default=None)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    token = iam_token()
    limits = fetch_quota(args.cloud_id, token)
    instances = fetch_instances(args.cloud_id, token)
    github_ok, pairs = True, []
    try:
        if not ensure_github_token():
            raise RuntimeError("GITHUB_TOKEN is unset and gh auth token failed")
        ghrun_n = sum(
            1 for instance in instances
            if isinstance(instance, dict) and instance.get("status") in (None, "RUNNING") and is_ghrun(instance)
        )
        last_incomplete = None
        for attempt in range(1, 4):
            pairs = collect_open_jobs(args.org, args.repo)
            busy_n, queued_n = count_assigned_and_queued(pairs)
            if not github_jobs_incomplete(ghrun_n, busy_n, queued_n):
                last_incomplete = None
                break
            last_incomplete = IncompleteGitHubJobs(
                "in_progress jobs cover %s/%s runners while %s jobs are queued" % (busy_n, ghrun_n, queued_n)
            )
            print("Warning: GitHub job list attempt %s/3: %s" % (attempt, last_incomplete))
        if last_incomplete:
            raise last_incomplete
    except Exception as exc:  # noqa: BLE001 — quota snapshot is still worth storing
        github_ok, pairs = False, None
        print("Warning: GitHub jobs were not loaded (%s); runners are state=unknown" % exc)
    specs, default = load_label_footprints()
    rows = build_snapshot_rows(
        now, limits, instances, pairs, load_fit_footprints(specs=specs, default=default),
        github_ok, label_specs=specs, label_default=default,
    )
    print(snapshot_summary(rows))
    if args.dry_run:
        print("dry-run; %d rows not written" % len(rows))
        return 0
    return write_rows(rows, table_path=args.table_path)


if __name__ == "__main__":
    raise SystemExit(main())
