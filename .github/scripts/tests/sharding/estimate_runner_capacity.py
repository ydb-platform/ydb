#!/usr/bin/env python3
"""Estimate free runner-pool capacity from cloud quota budgets.

Counts queued and in-progress GitHub Actions jobs whose runner labels are
listed in the capacity config, converts them to vCPU/RAM/disk/instance
demand using per-label VM footprints, subtracts the demand and the static
reserve from the folder quotas, and prints how many more runners of the
requested preset fit into the remaining budget.

The output (a single integer on stdout) is meant to be passed as
MAX_SHARDS to plan_shard_tests.sh. Exits 0 with no output when capacity
cannot be determined (API errors): the caller then runs uncapped.

Quotas, footprints and the reserve live in .github/config/runner_capacity.yml.
"""

import argparse
import json
import math
import os
import sys
import urllib.error
import urllib.request
from collections import Counter
from typing import Any

import yaml

RESOURCES = ("vcpu", "ram_gb", "nrd_ssd_gb")
ACTIVE_JOB_STATUSES = ("queued", "in_progress")
MAX_RUN_PAGES = 3


def api_get(url: str, token: str) -> Any:
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def list_active_runs(repo: str, token: str) -> list[dict[str, Any]]:
    runs: list[dict[str, Any]] = []
    for status in ("queued", "in_progress"):
        for page in range(1, MAX_RUN_PAGES + 1):
            data = api_get(
                f"https://api.github.com/repos/{repo}/actions/runs"
                f"?status={status}&per_page=100&page={page}",
                token,
            )
            chunk = data.get("workflow_runs", [])
            runs.extend(chunk)
            if len(chunk) < 100:
                break
    return runs


def count_active_jobs_by_label(
    repo: str, token: str, known_labels: set[str]
) -> Counter:
    """Count queued+in_progress jobs per recognized runner label."""
    demand: Counter = Counter()
    for run in list_active_runs(repo, token):
        for page in range(1, MAX_RUN_PAGES + 1):
            data = api_get(
                f"https://api.github.com/repos/{repo}/actions/runs/{run['id']}/jobs"
                f"?per_page=100&page={page}",
                token,
            )
            jobs = data.get("jobs", [])
            for job in jobs:
                if job.get("status") not in ACTIVE_JOB_STATUSES:
                    continue
                for label in job.get("labels", []):
                    if label in known_labels or label.startswith("build-preset-"):
                        demand[label] += 1
                        break
            if len(jobs) < 100:
                break
    return demand


def footprint_for(label: str, config: dict[str, Any]) -> dict[str, int]:
    return config["footprints"].get(label) or config["default_footprint"]


def compute_max_new_runners(
    demand: Counter, preset_label: str, config: dict[str, Any]
) -> tuple[int, dict[str, Any]]:
    quotas = config["quotas"]
    reserved = config.get("reserved", {})
    headroom = float(config.get("headroom_fraction", 1.0))

    used = {res: 0.0 for res in RESOURCES}
    used_instances = 0
    for label, count in demand.items():
        fp = footprint_for(label, config)
        for res in RESOURCES:
            used[res] += fp[res] * count
        used_instances += count

    free = {}
    for res in RESOURCES:
        budget = (quotas[res] - reserved.get(res, 0)) * headroom
        free[res] = budget - used[res]
    instances_budget = (quotas["instances"] - reserved.get("instances", 0)) * headroom
    free_instances = instances_budget - used_instances

    fp = footprint_for(preset_label, config)
    fits = [free_instances]
    for res in RESOURCES:
        fits.append(free[res] / fp[res])
    max_new = max(int(math.floor(min(fits))), 0)

    details = {
        "active_jobs_by_label": dict(demand),
        "used": {**{k: round(v) for k, v in used.items()}, "instances": used_instances},
        "free": {**{k: round(v) for k, v in free.items()}, "instances": round(free_instances)},
        "max_new_runners": max_new,
    }
    return max_new, details


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, help="runner_capacity.yml path")
    parser.add_argument(
        "--preset-label",
        required=True,
        help="runner label of the shards to schedule (e.g. build-preset-relwithdebinfo)",
    )
    parser.add_argument(
        "--repo", default=os.getenv("GITHUB_REPOSITORY", "ydb-platform/ydb")
    )
    args = parser.parse_args()

    with open(args.config, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    token = os.getenv("GITHUB_TOKEN") or os.getenv("GH_TOKEN")
    if not token:
        print("No GITHUB_TOKEN: skipping capacity estimate", file=sys.stderr)
        return 0

    try:
        demand = count_active_jobs_by_label(
            args.repo, token, set(config["footprints"])
        )
    except (urllib.error.URLError, OSError, json.JSONDecodeError) as e:
        print(f"Capacity estimate failed ({e}): running uncapped", file=sys.stderr)
        return 0

    max_new, details = compute_max_new_runners(demand, args.preset_label, config)
    print(json.dumps(details, indent=2), file=sys.stderr)

    floor = int(config.get("saturated_min_shards", 1))
    print(max(max_new, floor))
    return 0


if __name__ == "__main__":
    sys.exit(main())
