import os
import re
from pathlib import Path

import yaml


mnc_home = "/Berkanavt/multinode_home"
user = os.environ.get("USER")
if user:
    mnc_home = f"/home/{user}/multinode_home"

managed_partlabels = set()
managed_partlabel_regexps = []


def ensure_mnc_home():
    path = Path(mnc_home)
    path.mkdir(parents=True, exist_ok=True)
    return str(path)


def load_config(path: str):
    global mnc_home, managed_partlabels, managed_partlabel_regexps

    with open(path) as file:
        data = yaml.safe_load(file) or {}

    if data.get("mnc_home"):
        mnc_home = data["mnc_home"]

    disks = data.get("disks", {}) or {}
    managed_partlabels = set(data.get("managed_partlabels", []) or disks.get("managed_partlabels", []) or [])
    raw_regexps = (
        data.get("managed_partlabel_regexps", [])
        or disks.get("managed_partlabel_regexps", [])
        or disks.get("allowed_partlabel_regexps", [])
        or []
    )
    managed_partlabel_regexps = [re.compile(pattern) for pattern in raw_regexps]


def is_managed_partlabel(partlabel: str) -> bool:
    if not managed_partlabels and not managed_partlabel_regexps:
        return True
    return partlabel in managed_partlabels or any(pattern.fullmatch(partlabel) for pattern in managed_partlabel_regexps)
