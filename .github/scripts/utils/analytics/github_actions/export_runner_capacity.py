#!/usr/bin/env python3
"""Snapshot Yandex Cloud quota and the ghrun pool into analytics/ci_capacity.

Rows live one year (the same TTL as analytics/ci_metrics). One sample is
quota headroom, how many preset VMs still fit, each runner, and the Actions
queue. The Capacity tab in DataLens reads this table, not ci_metrics.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

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
# A value at least this large is bytes. 320 (GiB) stays GiB; 1 GiB in bytes does not.
BYTES_THRESHOLD = 1_000_000.0
QUOTA_FIELDS = (
    ("compute.instanceCores.count", "cores", False),
    ("compute.instanceMemory.size", "ram_gib", True),
    ("compute.instances.count", "instances", False),
    ("compute.ssdNonReplicatedDisks.size", "nrd_gib", True),
)
DEFAULT_FITS = {
    "relwithdebinfo": {"vcpu": 64.0, "ram_gb": 256.0, "nrd_ssd_gb": 2417.0},
    "release-asan": {"vcpu": 64.0, "ram_gb": 320.0, "nrd_ssd_gb": 2418.0},
}
DEFAULT_LABEL_FOOTPRINTS = {
    "build-preset-relwithdebinfo": {"vcpu": 64.0, "ram_gb": 256.0, "nrd_ssd_gb": 2417.0},
    "build-preset-release": {"vcpu": 64.0, "ram_gb": 256.0, "nrd_ssd_gb": 2417.0},
    "build-preset-release-asan": {"vcpu": 64.0, "ram_gb": 320.0, "nrd_ssd_gb": 2418.0},
    "build-preset-release-msan": {"vcpu": 64.0, "ram_gb": 320.0, "nrd_ssd_gb": 2417.0},
    "build-preset-release-tsan": {"vcpu": 64.0, "ram_gb": 320.0, "nrd_ssd_gb": 2417.0},
    "build-preset-analytic-node": {"vcpu": 8.0, "ram_gb": 64.0, "nrd_ssd_gb": 186.0},
}
DEFAULT_JOB_FOOTPRINT = {"vcpu": 96.0, "ram_gb": 320.0, "nrd_ssd_gb": 2417.0}
JOB_RANK = {"in_progress": 2, "queued": 1}
# GitHub-hosted images. They do not wait for ghrun and must not count as demand.
GITHUB_HOSTED_PREFIXES = ("ubuntu-", "windows-", "macos-")
GHRUN_NAME_PREFIXES = ("ghrun-", "static-")
# GitHub's status filter often returns a short page even when more runs exist.
# Stop only on an empty page, not on len(batch) < per_page.
RUN_PAGE_SIZE = 100
RUN_MAX_PAGES = 30


class IncompleteGitHubJobs(RuntimeError):
    """The Actions run list is truncated or internally inconsistent."""

COLUMNS = (
    ("sampled_at", "Timestamp", False),
    ("date", "Date", False),
    ("kind", "Utf8", False),
    ("name", "Utf8", False),
    ("workflow", "Utf8", True),
    ("job_name", "Utf8", True),
    ("branch", "Utf8", True),
    ("run_url", "Utf8", True),
    ("pr_number", "Utf8", True),
    ("pr_title", "Utf8", True),
    ("state", "Utf8", True),
    ("zone", "Utf8", True),
    ("cores", "Double", True),
    ("ram_gib", "Double", True),
    ("nrd_gib", "Double", True),
    ("limit_value", "Double", True),
    ("usage_value", "Double", True),
    ("free_value", "Double", True),
    ("created_at", "Timestamp", True),
    ("age_sec", "Double", True),
)
PRIMARY_KEYS = ("sampled_at", "date", "kind", "name")


def repo_root() -> Path:
    return Path(__file__).resolve().parents[5]


def footprints_path() -> Path:
    return repo_root() / ".github" / "config" / "runners_footprints.yml"


def resolve_capacity_table_path(ydb_wrapper=None) -> str:
    return resolve_table_path(ydb_wrapper, table_config_key=TABLE_CONFIG_KEY, default=DEFAULT_TABLE_PATH)


def build_create_capacity_table_sql(table_path: str, ttl_minutes: int = TTL_MINUTES) -> str:
    """Column table. TTL is one year on sampled_at (TTL_MINUTES == 365 days)."""
    col_defs = []
    for name, sql_type, nullable in COLUMNS:
        null_str = "" if nullable else " NOT NULL"
        col_defs.append(f"            `{name}` {sql_type}{null_str}")
    columns_sql = ",\n".join(col_defs)
    pk_sql = ", ".join("`%s`" % key for key in PRIMARY_KEYS)
    return f"""
        CREATE TABLE IF NOT EXISTS `{table_path}` (
{columns_sql},
            PRIMARY KEY ({pk_sql})
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
    if value >= BYTES_THRESHOLD:
        return value / GIB
    return value


def load_fit_footprints(path: Optional[Path] = None) -> Dict[str, Dict[str, float]]:
    path = path or footprints_path()
    out = {label: dict(spec) for label, spec in DEFAULT_FITS.items()}
    if not path.is_file():
        return out
    try:
        import yaml
    except ImportError:
        return out
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError):
        return out
    footprints = data.get("footprints") or {}
    mapping = {
        "relwithdebinfo": "build-preset-relwithdebinfo",
        "release-asan": "build-preset-release-asan",
    }
    for label, key in mapping.items():
        raw = footprints.get(key) or {}
        spec = out[label]
        for src, dest in (("vcpu", "vcpu"), ("ram_gb", "ram_gb"), ("nrd_ssd_gb", "nrd_ssd_gb")):
            number = as_float(raw.get(src))
            if number is not None and number > 0:
                spec[dest] = number
    return out


def quota_rows_for(limits: List[Dict[str, Any]], quota_id: str) -> List[Dict[str, Any]]:
    """Cloud-wide id, or the same id plus a zone suffix after YC split quotas per AZ."""
    prefix = quota_id + "."
    out: List[Dict[str, Any]] = []
    for row in limits:
        if not isinstance(row, dict):
            continue
        name = row.get("quota_id") or ""
        if name == quota_id or name.startswith(prefix):
            out.append(row)
    return out


def quota_az(quota_id: str, field: str) -> Optional[str]:
    """AZ after the field id, empty string for the cloud-wide id, else None."""
    if quota_id == field:
        return ""
    prefix = field + "."
    if quota_id.startswith(prefix):
        return quota_id[len(prefix):]
    return None


def quota_headroom(limits: List[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    out: Dict[str, Dict[str, float]] = {}
    for quota_id, name, as_bytes in QUOTA_FIELDS:
        limit_total = 0.0
        usage_total = 0.0
        found = False
        for row in quota_rows_for(limits, quota_id):
            limit = as_float(row.get("limit"))
            usage = as_float(row.get("usage"))
            if limit is None or usage is None:
                continue
            if as_bytes:
                limit = limit / GIB
                usage = usage / GIB
            limit_total += limit
            usage_total += usage
            found = True
        if found:
            out[name] = {"limit": limit_total, "usage": usage_total, "free": limit_total - usage_total}
    return out


def quota_headroom_by_zone(limits: List[Dict[str, Any]]) -> Dict[str, Dict[str, Dict[str, float]]]:
    """Per-AZ headroom. Cloud-wide ids (no AZ suffix) are skipped here."""
    out: Dict[str, Dict[str, Dict[str, float]]] = {}
    for quota_id, name, as_bytes in QUOTA_FIELDS:
        for row in limits:
            if not isinstance(row, dict):
                continue
            az = quota_az(row.get("quota_id") or "", quota_id)
            if not az:
                continue
            limit = as_float(row.get("limit"))
            usage = as_float(row.get("usage"))
            if limit is None or usage is None:
                continue
            if as_bytes:
                limit = limit / GIB
                usage = usage / GIB
            bucket = out.setdefault(az, {})
            spec = bucket.setdefault(name, {"limit": 0.0, "usage": 0.0, "free": 0.0})
            spec["limit"] += limit
            spec["usage"] += usage
            spec["free"] = spec["limit"] - spec["usage"]
    return out


def hosts_that_fit(headroom: Dict[str, Dict[str, float]], footprint: Dict[str, float]) -> int:
    bounds = []
    cores = headroom.get("cores")
    ram = headroom.get("ram_gib")
    instances = headroom.get("instances")
    nrd = headroom.get("nrd_gib")
    if cores is not None and footprint.get("vcpu"):
        bounds.append(int(cores["free"] // footprint["vcpu"]))
    if ram is not None and footprint.get("ram_gb"):
        bounds.append(int(ram["free"] // footprint["ram_gb"]))
    if instances is not None:
        bounds.append(int(instances["free"]))
    if nrd is not None and footprint.get("nrd_ssd_gb"):
        bounds.append(int(nrd["free"] // footprint["nrd_ssd_gb"]))
    if not bounds:
        return 0
    return max(min(bounds), 0)


def is_ghrun(instance: Dict[str, Any]) -> bool:
    labels = instance.get("labels") or {}
    if isinstance(labels, dict) and "ghrun" in labels:
        return True
    name = instance.get("name") or ""
    return name.startswith(GHRUN_NAME_PREFIXES)


def instance_zone(instance: Dict[str, Any]) -> str:
    raw = instance.get("zoneId") or instance.get("zone_id") or instance.get("zone") or ""
    return str(raw).strip()


def instance_cores(instance: Dict[str, Any]) -> Optional[float]:
    resources = instance.get("resources") or {}
    return as_float(resources.get("cores", instance.get("cores")))


def instance_ram_gib(instance: Dict[str, Any]) -> Optional[float]:
    resources = instance.get("resources") or {}
    raw = as_float(resources.get("memory", instance.get("ram")))
    if raw is None:
        return None
    return bytes_to_gib(raw)


def age_sec(now: datetime, moment: Optional[datetime]) -> Optional[float]:
    if moment is None:
        return None
    return max((now - moment).total_seconds(), 0.0)


def _blank(
    sampled_at: datetime,
    kind: str,
    name: str,
    **fields: Any,
) -> Dict[str, Any]:
    row = {
        "sampled_at": sampled_at,
        "date": sampled_at.date(),
        "kind": kind,
        "name": name,
        "workflow": None,
        "job_name": None,
        "branch": None,
        "run_url": None,
        "pr_number": None,
        "pr_title": None,
        "state": None,
        "zone": None,
        "cores": None,
        "ram_gib": None,
        "nrd_gib": None,
        "limit_value": None,
        "usage_value": None,
        "free_value": None,
        "created_at": None,
        "age_sec": None,
    }
    row.update(fields)
    return row


PR_EVENTS = ("pull_request", "pull_request_target")


def pull_info(run: Dict[str, Any]) -> tuple:
    """PR number and title when the run belongs to a pull request."""
    if (run or {}).get("event") not in PR_EVENTS:
        return None, None
    pulls = (run or {}).get("pull_requests") or []
    if not pulls or not isinstance(pulls[0], dict):
        return None, None
    pull = pulls[0]
    number = pull.get("number")
    if number is None:
        return None, None
    title = pull.get("title") or ""
    if not title:
        title = (run or {}).get("display_title") or ""
    return str(number), (str(title) or None)


def target_branch(run: Dict[str, Any]) -> str:
    """PR target branch when the run has one, otherwise the branch the run is on."""
    pulls = (run or {}).get("pull_requests") or []
    if pulls and isinstance(pulls[0], dict):
        base = pulls[0].get("base") or {}
        if isinstance(base, dict) and base.get("ref"):
            return str(base["ref"])
    head = (run or {}).get("head_branch") or ""
    return str(head)


def index_runner_jobs(pairs: List[tuple]) -> Dict[str, Dict[str, Any]]:
    """One job per runner. An in-progress job wins over a queued one."""
    best: Dict[str, Dict[str, Any]] = {}
    for run, job in pairs:
        if not isinstance(job, dict):
            continue
        runner = job.get("runner_name") or ""
        status = job.get("status") or ""
        if not runner or status not in JOB_RANK:
            continue
        job_name = job.get("name") or ""
        if "${{" in job_name:
            continue
        current = best.get(runner)
        if current is not None and JOB_RANK[status] <= JOB_RANK[current["status"]]:
            continue
        number, title = pull_info(run or {})
        best[runner] = {
            "status": status,
            "workflow": (run or {}).get("name") or "",
            "job_name": job_name,
            "branch": target_branch(run or {}),
            "run_url": job.get("html_url") or (run or {}).get("html_url") or "",
            "pr_number": number,
            "pr_title": title,
            "created_at": parse_datetime(job.get("created_at")),
        }
    return best


def load_label_footprints(path: Optional[Path] = None):
    """Runner size by GitHub label, plus the size used when the label is unknown."""
    path = path or footprints_path()
    specs = {label: dict(spec) for label, spec in DEFAULT_LABEL_FOOTPRINTS.items()}
    default = dict(DEFAULT_JOB_FOOTPRINT)
    if not path.is_file():
        return specs, default
    try:
        import yaml
    except ImportError:
        return specs, default
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError):
        return specs, default
    for label, raw in (data.get("footprints") or {}).items():
        if not isinstance(raw, dict):
            continue
        specs[str(label)] = {
            "vcpu": as_float(raw.get("vcpu")) or default["vcpu"],
            "ram_gb": as_float(raw.get("ram_gb")) or default["ram_gb"],
            "nrd_ssd_gb": as_float(raw.get("nrd_ssd_gb")) or default["nrd_ssd_gb"],
        }
    raw_default = data.get("default_footprint") or {}
    if isinstance(raw_default, dict):
        default = {
            "vcpu": as_float(raw_default.get("vcpu")) or default["vcpu"],
            "ram_gb": as_float(raw_default.get("ram_gb")) or default["ram_gb"],
            "nrd_ssd_gb": as_float(raw_default.get("nrd_ssd_gb")) or default["nrd_ssd_gb"],
        }
    return specs, default


def job_label_names(job: Dict[str, Any]) -> List[str]:
    names: List[str] = []
    for label in job.get("labels") or []:
        if isinstance(label, dict):
            label = label.get("name") or ""
        text = str(label).strip()
        if text:
            names.append(text)
    return names


def is_github_hosted_job(job: Dict[str, Any]) -> bool:
    """True for ubuntu-latest and other GitHub-hosted images.

    Those jobs never take a ghrun VM. If a job also asks for self-hosted
    or a build-preset, keep it: that is our pool.
    """
    labels = [name.lower() for name in job_label_names(job)]
    if "self-hosted" in labels or any(name.startswith("build-preset-") for name in labels):
        return False
    return any(name.startswith(GITHUB_HOSTED_PREFIXES) for name in labels)


def job_footprint(job: Dict[str, Any], specs: Dict[str, Dict[str, float]], default: Dict[str, float]) -> Dict[str, float]:
    """Size of the runner a queued job will ask for."""
    labels = job_label_names(job)
    for label in labels:
        if label in specs:
            return specs[label]
        prefixed = "build-preset-" + label
        if prefixed in specs:
            return specs[prefixed]
    blob = ((job.get("name") or "") + " " + " ".join(labels)).lower()
    if any(token in blob for token in ("asan", "msan", "tsan")):
        return specs.get("build-preset-release-asan", default)
    if "relwithdebinfo" in blob or "release" in blob:
        return specs.get("build-preset-relwithdebinfo", default)
    return default


def queue_row_name(run: Dict[str, Any], job: Dict[str, Any]) -> str:
    run_id = (run or {}).get("id")
    job_id = job.get("id")
    if run_id is not None and job_id is not None:
        return "q.%s.%s" % (run_id, job_id)
    workflow = (run or {}).get("name") or "wf"
    job_name = job.get("name") or "job"
    return "q.%s.%s" % (workflow, job_name)


def queue_jobs(
    pairs: List[tuple],
    now: datetime,
    specs: Optional[Dict[str, Dict[str, float]]] = None,
    default: Optional[Dict[str, float]] = None,
) -> List[Dict[str, Any]]:
    """One row per queued job, with the GitHub link so the dash can open it."""
    specs = specs or {}
    default = default or dict(DEFAULT_JOB_FOOTPRINT)
    jobs: List[Dict[str, Any]] = []
    used = set()
    for run, job in pairs:
        if not isinstance(job, dict) or job.get("status") != "queued":
            continue
        if is_github_hosted_job(job):
            continue
        job_name = job.get("name") or ""
        if "${{" in job_name:
            continue
        workflow = (run or {}).get("name") or "(no workflow)"
        created = parse_datetime(job.get("created_at") or (run or {}).get("created_at"))
        size = job_footprint(job, specs, default)
        number, title = pull_info(run or {})
        name = queue_row_name(run or {}, job)
        if name in used:
            name = "%s.%s" % (name, len(used))
        used.add(name)
        jobs.append({
            "name": name,
            "workflow": workflow,
            "job_name": job_name,
            "branch": target_branch(run or {}) or None,
            "run_url": job.get("html_url") or (run or {}).get("html_url") or None,
            "pr_number": number,
            "pr_title": title,
            "cores": size["vcpu"],
            "ram": size["ram_gb"],
            "nrd": size["nrd_ssd_gb"],
            "created_at": created,
            "age": age_sec(now, created),
        })
    jobs.sort(key=lambda item: (-(item["age"] or 0), item["workflow"], item["job_name"]))
    return jobs


def count_assigned_and_queued(pairs: Optional[List[tuple]]) -> tuple:
    """How many runners have an in-progress job, and how many jobs are queued."""
    busy = set()
    queued = 0
    for _run, job in pairs or []:
        if not isinstance(job, dict):
            continue
        status = job.get("status") or ""
        runner = job.get("runner_name") or ""
        if status == "in_progress" and runner:
            busy.add(runner)
        elif status == "queued" and not is_github_hosted_job(job):
            queued += 1
    return len(busy), queued


def github_jobs_incomplete(ghrun_n: int, busy_n: int, queued_n: int) -> bool:
    """True when a short in_progress list plus a long queue cannot be real.

    That shape is what a truncated Actions page looks like: VMs stay in the
    pool, but we never saw the jobs on them, so they are stored as idle.
    """
    if ghrun_n < 10:
        return False
    if queued_n >= 20 and busy_n < ghrun_n * 0.35:
        return True
    # Truncated in_progress list: a large pool with almost no assigned jobs.
    return ghrun_n >= 40 and busy_n < ghrun_n * 0.15


def runner_state(name: str, jobs: Optional[Dict[str, Dict[str, Any]]], github_ok: bool) -> str:
    if not github_ok or jobs is None:
        return "unknown"
    job = jobs.get(name)
    if not job:
        return "idle"
    if job["status"] == "in_progress":
        return "busy"
    if job["status"] == "queued":
        return "queued"
    return "idle"


def unique_name(instance: Dict[str, Any], used: set) -> str:
    name = instance.get("name") or instance.get("id") or "unnamed"
    if name not in used:
        used.add(name)
        return name
    alt = "%s@%s" % (name, instance.get("_folder_id") or "folder")
    used.add(alt)
    return alt


def quota_and_fit_rows(
    sampled_at: datetime,
    headroom: Dict[str, Dict[str, float]],
    footprints: Dict[str, Dict[str, float]],
    zone: str,
) -> List[Dict[str, Any]]:
    suffix = "@%s" % zone if zone else ""
    rows: List[Dict[str, Any]] = []
    for name, spec in headroom.items():
        rows.append(
            _blank(
                sampled_at,
                "quota",
                name + suffix,
                zone=zone or None,
                limit_value=spec["limit"],
                usage_value=spec["usage"],
                free_value=spec["free"],
                cores=spec["free"] if name == "cores" else None,
                ram_gib=spec["free"] if name == "ram_gib" else None,
                nrd_gib=spec["free"] if name == "nrd_gib" else None,
            )
        )
    for label, footprint in footprints.items():
        rows.append(
            _blank(
                sampled_at,
                "fit",
                "fit.%s%s" % (label, suffix),
                zone=zone or None,
                free_value=float(hosts_that_fit(headroom, footprint)),
                cores=footprint.get("vcpu"),
                ram_gib=footprint.get("ram_gb"),
                nrd_gib=footprint.get("nrd_ssd_gb"),
            )
        )
    return rows


def append_runner_summary(
    rows: List[Dict[str, Any]],
    sampled_at: datetime,
    ghruns: List[Dict[str, Any]],
    zone: str,
) -> None:
    suffix = "@%s" % zone if zone else ""
    created = [item for item in ghruns if item["created_at"] is not None]
    newest = max(created, key=lambda item: item["created_at"]) if created else None
    born = [item for item in created if sampled_at - item["created_at"] <= timedelta(hours=24)]
    idle = [item for item in ghruns if item["state"] == "idle"]
    busy = [item for item in ghruns if item["state"] == "busy"]
    rows.append(
        _blank(
            sampled_at,
            "summary",
            "runners.ghrun" + suffix,
            zone=zone or None,
            usage_value=float(len(ghruns)),
            free_value=float(len(idle)),
            cores=sum(item["cores"] or 0 for item in busy),
            ram_gib=sum(item["ram"] or 0 for item in busy),
            limit_value=sum(item["ram"] or 0 for item in ghruns),
        )
    )
    rows.append(
        _blank(
            sampled_at,
            "summary",
            "runners.idle" + suffix,
            zone=zone or None,
            usage_value=float(len(idle)),
            cores=sum(item["cores"] or 0 for item in idle),
            ram_gib=sum(item["ram"] or 0 for item in idle),
        )
    )
    rows.append(
        _blank(
            sampled_at,
            "summary",
            "runners.created_24h" + suffix,
            zone=zone or None,
            usage_value=float(len(born)),
        )
    )
    if newest:
        rows.append(
            _blank(
                sampled_at,
                "summary",
                "runners.newest" + suffix,
                zone=zone or None,
                job_name=newest["name"],
                created_at=newest["created_at"],
                age_sec=age_sec(sampled_at, newest["created_at"]),
                usage_value=float(len(ghruns)),
            )
        )


def build_snapshot_rows(
    now: datetime,
    limits: List[Dict[str, Any]],
    instances: List[Dict[str, Any]],
    pairs: Optional[List[tuple]],
    footprints: Dict[str, Dict[str, float]],
    github_ok: bool,
) -> List[Dict[str, Any]]:
    sampled_at = now.replace(microsecond=0)
    rows: List[Dict[str, Any]] = []
    headroom = quota_headroom(limits)
    rows.extend(quota_and_fit_rows(sampled_at, headroom, footprints, zone=""))
    for az, zonal in sorted(quota_headroom_by_zone(limits).items()):
        rows.extend(quota_and_fit_rows(sampled_at, zonal, footprints, zone=az))

    jobs = index_runner_jobs(pairs) if github_ok and pairs is not None else None
    used: set = set()
    ghruns: List[Dict[str, Any]] = []
    for instance in instances:
        if not isinstance(instance, dict):
            continue
        status = instance.get("status")
        if status not in (None, "RUNNING"):
            continue
        name = unique_name(instance, used)
        cores = instance_cores(instance)
        ram = instance_ram_gib(instance)
        created = parse_datetime(instance.get("created_at"))
        zone = instance_zone(instance) or None
        if is_ghrun(instance):
            state = runner_state(name, jobs, github_ok)
            job = (jobs or {}).get(name) or {}
            ghruns.append({
                "name": name,
                "created_at": created,
                "state": state,
                "cores": cores,
                "ram": ram,
                "zone": zone or "",
            })
            rows.append(
                _blank(
                    sampled_at,
                    "runner",
                    name,
                    workflow=job.get("workflow"),
                    job_name=job.get("job_name"),
                    branch=job.get("branch") or None,
                    run_url=job.get("run_url") or None,
                    pr_number=job.get("pr_number") or None,
                    pr_title=job.get("pr_title") or None,
                    state=state,
                    zone=zone,
                    cores=cores,
                    ram_gib=ram,
                    created_at=created,
                    age_sec=age_sec(sampled_at, created),
                )
            )
        else:
            rows.append(
                _blank(
                    sampled_at,
                    "runner",
                    name,
                    state="other",
                    zone=zone,
                    job_name=instance.get("_folder_name") or None,
                    cores=cores,
                    ram_gib=ram,
                    created_at=created,
                    age_sec=age_sec(sampled_at, created),
                )
            )

    append_runner_summary(rows, sampled_at, ghruns, "")
    by_zone: Dict[str, List[Dict[str, Any]]] = {}
    for item in ghruns:
        az = item.get("zone") or ""
        if az:
            by_zone.setdefault(az, []).append(item)
    for az in sorted(by_zone):
        append_runner_summary(rows, sampled_at, by_zone[az], az)

    if github_ok and pairs is not None:
        label_specs, label_default = load_label_footprints()
        for item in queue_jobs(pairs, sampled_at, label_specs, label_default):
            rows.append(
                _blank(
                    sampled_at,
                    "queue",
                    item["name"],
                    workflow=item["workflow"],
                    job_name=item["job_name"] or None,
                    branch=item["branch"],
                    run_url=item["run_url"],
                    pr_number=item["pr_number"],
                    pr_title=item["pr_title"],
                    usage_value=1.0,
                    age_sec=item["age"],
                    created_at=item["created_at"],
                    cores=item["cores"],
                    ram_gib=item["ram"],
                    nrd_gib=item["nrd"],
                )
            )
    return rows


def snapshot_summary(rows: List[Dict[str, Any]]) -> str:
    by_key = {(row["kind"], row["name"]): row for row in rows}
    def free(name: str) -> str:
        row = by_key.get(("quota", name))
        if not row:
            return "?"
        return "%.0f" % row["free_value"]

    fit_rel = by_key.get(("fit", "fit.relwithdebinfo"))
    fit_asan = by_key.get(("fit", "fit.release-asan"))
    ghrun = by_key.get(("summary", "runners.ghrun"))
    newest = by_key.get(("summary", "runners.newest"))
    queued = [row for row in rows if row["kind"] == "queue"]
    queued_n = sum(row["usage_value"] or 0 for row in queued)
    newest_at = newest["created_at"].isoformat() if newest and newest["created_at"] else "?"
    return (
        "ram_free=%sGiB cores_free=%s instances_free=%s nrd_free=%sGiB "
        "fit_relwithdebinfo=%s fit_asan=%s ghrun=%s idle=%s queued_jobs=%.0f newest=%s"
        % (
            free("ram_gib"),
            free("cores"),
            free("instances"),
            free("nrd_gib"),
            "%.0f" % fit_rel["free_value"] if fit_rel else "?",
            "%.0f" % fit_asan["free_value"] if fit_asan else "?",
            "%.0f" % ghrun["usage_value"] if ghrun else "?",
            "%.0f" % ghrun["free_value"] if ghrun else "?",
            queued_n,
            newest_at,
        )
    )


def yc_json(args: List[str], env: Dict[str, str]) -> Any:
    proc = subprocess.run(
        ["yc", *args, "--format", "json"],
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip().splitlines()
        tail = detail[-1] if detail else "yc exited %s" % proc.returncode
        raise RuntimeError(tail)
    return json.loads(proc.stdout)


def open_yc(cloud_id: str):
    """Use the current yc profile. Fall back to the CI key in a temp HOME."""
    env = os.environ.copy()
    try:
        configured = subprocess.check_output(
            ["yc", "config", "get", "cloud-id"],
            env=env,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        configured = ""
    if configured:
        return env, None, configured
    key = os.environ.get("CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS") or ""
    if not key or not os.path.isfile(key):
        raise RuntimeError("yc has no cloud-id and the CI service-account key file is missing")
    home = tempfile.mkdtemp(prefix="yc-capacity-")
    env = os.environ.copy()
    env["HOME"] = home
    env.pop("YC_TOKEN", None)
    subprocess.check_call(
        ["yc", "config", "set", "service-account-key", key],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    subprocess.check_call(
        ["yc", "config", "set", "cloud-id", cloud_id],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return env, home, cloud_id


def fetch_quota(env: Dict[str, str], cloud_id: str) -> List[Dict[str, Any]]:
    payload = yc_json(
        [
            "quota-manager",
            "quota-limit",
            "list",
            "--service",
            "compute",
            "--resource-type",
            "resource-manager.cloud",
            "--resource-id",
            cloud_id,
        ],
        env,
    )
    if isinstance(payload, dict):
        return list(payload.get("quota_limits") or [])
    return list(payload or [])


def fetch_instances(env: Dict[str, str], cloud_id: str) -> List[Dict[str, Any]]:
    folders = yc_json(["resource-manager", "folder", "list", "--cloud-id", cloud_id], env)
    if isinstance(folders, dict):
        folders = folders.get("folders") or []
    instances: List[Dict[str, Any]] = []
    for folder in folders:
        if not isinstance(folder, dict):
            continue
        if folder.get("status") not in (None, "ACTIVE"):
            continue
        batch = yc_json(["compute", "instance", "list", "--folder-id", folder["id"]], env)
        if isinstance(batch, dict):
            batch = batch.get("instances") or []
        for instance in batch:
            if not isinstance(instance, dict):
                continue
            copied = dict(instance)
            copied["_folder_id"] = folder.get("id") or ""
            copied["_folder_name"] = folder.get("name") or ""
            instances.append(copied)
    return instances


def ensure_github_token() -> bool:
    if os.environ.get("GITHUB_TOKEN"):
        return True
    try:
        token = subprocess.check_output(
            ["gh", "auth", "token"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return False
    if not token:
        return False
    os.environ["GITHUB_TOKEN"] = token
    return True


def list_workflow_runs(
    org: str,
    repo: str,
    status: str,
    per_page: int = RUN_PAGE_SIZE,
    max_pages: int = RUN_MAX_PAGES,
    fetcher=None,
) -> List[Dict[str, Any]]:
    """All runs with this status. A short page is not the end of the list."""
    get = fetcher or github_get
    url = "https://api.github.com/repos/%s/%s/actions/runs" % (org, repo)
    runs: List[Dict[str, Any]] = []
    seen = set()
    last_batch: List[Dict[str, Any]] = []
    empty_streak = 0
    for page in range(1, max_pages + 1):
        payload = get(url, params={"status": status, "per_page": per_page, "page": page})
        last_batch = payload.get("workflow_runs") or []
        if not last_batch:
            empty_streak += 1
            if empty_streak >= 2:
                return runs
            continue
        empty_streak = 0
        for run in last_batch:
            run_id = run.get("id")
            if run_id in seen:
                continue
            seen.add(run_id)
            runs.append(run)
    raise IncompleteGitHubJobs(
        "workflow runs for status=%s hit max_pages=%s after %s runs (last page %s)"
        % (status, max_pages, len(runs), len(last_batch))
    )


def enrich_run_pull(
    org: str,
    repo: str,
    run: Dict[str, Any],
    cache: Dict[str, list],
    fetcher=None,
) -> Dict[str, Any]:
    """Fill the PR number and target branch. Scheduled runs carry unrelated pull_requests."""
    event = run.get("event") or ""
    if event not in PR_EVENTS:
        if run.get("pull_requests"):
            cleaned = dict(run)
            cleaned["pull_requests"] = []
            return cleaned
        return run
    return attach_pull_requests(org, repo, run, cache=cache, fetcher=fetcher)


def collect_open_jobs(org: str, repo: str) -> List[tuple]:
    pairs: List[tuple] = []
    cache: Dict[str, list] = {}
    for status in ("in_progress", "queued"):
        for run in list_workflow_runs(org, repo, status):
            run_id = run.get("id")
            if run_id is None:
                continue
            run = enrich_run_pull(org, repo, run, cache)
            for job in list_run_jobs(org, repo, int(run_id)):
                pairs.append((run, job))
    return pairs


def ensure_optional_column(wrapper, path: str, column: str, sql_type: str) -> None:
    """CREATE TABLE does not add columns to a table that already exists."""
    try:
        wrapper.create_table(
            path,
            "ALTER TABLE `%s` ADD COLUMN `%s` %s;" % (path, column, sql_type),
        )
    except Exception as exc:  # noqa: BLE001 — the column is already there after the first run
        text = str(exc).lower()
        if "exist" in text or "already" in text or "duplicate" in text:
            return
        raise


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
        ensure_optional_column(wrapper, path, "branch", "Utf8")
        ensure_optional_column(wrapper, path, "run_url", "Utf8")
        ensure_optional_column(wrapper, path, "pr_number", "Utf8")
        ensure_optional_column(wrapper, path, "pr_title", "Utf8")
        ensure_optional_column(wrapper, path, "zone", "Utf8")
        wrapper.bulk_upsert_batches(path, rows, build_column_types(COLUMNS), batch_size=500)
    print("Wrote %d rows to %s" % (len(rows), path))
    return 0


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Snapshot Cloud quota and ghrun capacity into YDB")
    parser.add_argument("--org", default=os.environ.get("CI_METRICS_ORG", DEFAULT_ORG))
    parser.add_argument("--repo", default=os.environ.get("CI_METRICS_REPO", DEFAULT_REPO))
    parser.add_argument("--cloud-id", default=os.environ.get("YC_CLOUD_ID", DEFAULT_CLOUD_ID))
    parser.add_argument("--table-path", default=None)
    parser.add_argument("--dry-run", action="store_true", help="Print the summary and do not write YDB")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    env, home, cloud_id = open_yc(args.cloud_id)
    try:
        limits = fetch_quota(env, cloud_id)
        instances = fetch_instances(env, cloud_id)
    finally:
        if home:
            shutil.rmtree(home, ignore_errors=True)
    github_ok = True
    pairs: Optional[List[tuple]] = []
    try:
        if not ensure_github_token():
            raise RuntimeError("GITHUB_TOKEN is unset and gh auth token failed")
        ghrun_n = sum(
            1
            for instance in instances
            if isinstance(instance, dict)
            and instance.get("status") in (None, "RUNNING")
            and is_ghrun(instance)
        )
        last_incomplete = None
        pairs = []
        for attempt in range(1, 4):
            pairs = collect_open_jobs(args.org, args.repo)
            busy_n, queued_n = count_assigned_and_queued(pairs)
            if not github_jobs_incomplete(ghrun_n, busy_n, queued_n):
                last_incomplete = None
                break
            last_incomplete = IncompleteGitHubJobs(
                "in_progress jobs cover %s/%s runners while %s jobs are queued"
                % (busy_n, ghrun_n, queued_n)
            )
            print("Warning: GitHub job list attempt %s/3: %s" % (attempt, last_incomplete))
        if last_incomplete:
            raise last_incomplete
    except Exception as exc:  # noqa: BLE001 — quota snapshot is still worth storing
        github_ok = False
        pairs = None
        print("Warning: GitHub jobs were not loaded (%s); runners are state=unknown" % exc)
    rows = build_snapshot_rows(
        now,
        limits,
        instances,
        pairs,
        load_fit_footprints(),
        github_ok,
    )
    print(snapshot_summary(rows))
    if args.dry_run:
        print("dry-run; %d rows not written" % len(rows))
        return 0
    return write_rows(rows, table_path=args.table_path)


if __name__ == "__main__":
    raise SystemExit(main())
