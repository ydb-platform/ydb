#!/usr/bin/env python3
"""Create a long-lived GitHub Actions runner VM from a flat config file.

Cloud-init only prepares the host. The runner is registered over SSH afterwards,
so GitHub labels can be changed by re-running ``register`` on the same VM.
"""

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REGISTER_SCRIPT = HERE / "register_runner.sh"

DEFAULTS = {
    "repo": "ydb-platform/ydb",
    "folder_id": "b1grf3mpoatgflnlavjd",
    "zone": "ru-central1-d",
    "subnet_id": "fl8gu1e1h8kqnlfj4uqp",
    "service_account_id": "ajelpris7av8gvq3ajum",
    "image_family": "gh-runner-ubuntu-2204",
    "disk_type": "network-ssd-nonreplicated",
    "ssh_public_key": "~/.ssh/id_ed25519.pub",
    "ssh_identity": "~/.ssh/id_ed25519",
    "ssh_public_keys": [],
    "jump_host": "gh-runners-gw",
    "ssh_user": "ubuntu",
    "preset": "",
    "agent_mirror": "",
    "count": 1,
    "start": 1,
    "index_width": 0,
    "public_ip": False,
    "public_address": "",
}

REQUIRED = ("platform", "cores", "memory_gb", "disk_gb", "labels")
NAME_RE = re.compile(r"^[a-z][a-z0-9-]{0,62}$")
NRD_CHUNK_GB = 93


class ConfigError(Exception):
    pass


def parse_scalar(raw):
    if raw == "" or raw is None:
        return ""
    if (raw.startswith('"') and raw.endswith('"')) or (raw.startswith("'") and raw.endswith("'")):
        return raw[1:-1]
    if raw in ("true", "false"):
        return raw == "true"
    if raw in ("null", "~"):
        return None
    if re.fullmatch(r"-?\d+", raw):
        return int(raw)
    return raw


def parse_yaml(text):
    """Flat mapping plus lists of scalars. Comments and blank lines are ignored."""
    root = {}
    current_list = None
    for lineno, raw in enumerate(text.splitlines(), 1):
        if not raw.strip() or raw.lstrip().startswith("#"):
            current_list = None
            continue
        if raw.startswith(" ") or raw.startswith("\t"):
            item = raw.strip()
            if current_list is None or not item.startswith("- "):
                raise ConfigError(f"line {lineno}: expected a list item under the previous key")
            current_list.append(parse_scalar(item[2:].strip()))
            continue
        current_list = None
        if ":" not in raw:
            raise ConfigError(f"line {lineno}: expected key: value")
        key, value = raw.split(":", 1)
        key = key.strip()
        value = value.strip()
        if value == "":
            root[key] = []
            current_list = root[key]
        elif value.startswith("[") and value.endswith("]"):
            inner = value[1:-1].strip()
            root[key] = [] if not inner else [parse_scalar(part.strip()) for part in inner.split(",")]
        else:
            root[key] = parse_scalar(value)
    return root


def load_config(path):
    path = Path(path)
    data = parse_yaml(path.read_text())
    fleet_keys = None
    if "shape" in data:
        fleet_keys = set(data) - {"shape"}
        shape_path = (path.parent / str(data["shape"])).resolve()
        if not shape_path.is_file():
            raise ConfigError(f"shape not found: {data['shape']}")
        shape = parse_yaml(shape_path.read_text())
        forbidden = set(shape) & {"labels", "count", "name", "names", "start", "shape"}
        if forbidden:
            raise ConfigError(f"shape {shape_path.name} must not set {', '.join(sorted(forbidden))}")
        data = {**shape, **{key: value for key, value in data.items() if key != "shape"}}
    unknown = sorted(set(data) - set(DEFAULTS) - set(REQUIRED) - {"name", "names"})
    if unknown:
        raise ConfigError(f"unknown keys: {', '.join(unknown)}")
    if "name" in data and "names" in data:
        raise ConfigError("set either name or names, not both")
    cfg = dict(DEFAULTS)
    cfg.update(data)
    if "folder_id" not in data:
        env_folder = os.environ.get("YC_FOLDER_ID", "").strip()
        if env_folder:
            cfg["folder_id"] = env_folder
    cfg["_explicit"] = set(data)
    cfg["_pool"] = path.stem
    cfg["_fleet"] = fleet_keys is not None
    if cfg["_fleet"]:
        if "names" in data:
            raise ConfigError("fleet file sets count, labels, and a name prefix, not a runner list")
        prefix = data.get("name")
        if "name" not in (fleet_keys or set()) or not isinstance(prefix, str) or not NAME_RE.fullmatch(prefix):
            raise ConfigError("fleet file must set name to a VM name prefix")
    elif "name" not in data and "names" not in data:
        raise ConfigError("set name or names")
    for key in REQUIRED:
        if key not in data:
            raise ConfigError(f"missing {key}")
    if not isinstance(cfg["labels"], list) or not cfg["labels"]:
        raise ConfigError("labels must be a non-empty list")
    for label in cfg["labels"]:
        if not isinstance(label, str) or not label or "," in label:
            raise ConfigError(f"bad label: {label!r}")
    for key in ("cores", "memory_gb", "disk_gb", "count", "start", "index_width"):
        if not isinstance(cfg[key], int) or isinstance(cfg[key], bool):
            raise ConfigError(f"{key} must be an integer")
    if cfg["count"] < 1 or cfg["start"] < 0 or cfg["index_width"] < 0:
        raise ConfigError("count, start and index_width must be non-negative; count at least 1")
    if cfg["_fleet"]:
        if fleet_keys is None or "count" not in fleet_keys or "labels" not in fleet_keys:
            raise ConfigError("fleet file must set count and labels")
    extra_keys = cfg["ssh_public_keys"]
    if extra_keys is None:
        cfg["ssh_public_keys"] = []
    elif not isinstance(extra_keys, list) or any(not isinstance(item, str) or not item for item in extra_keys):
        raise ConfigError("ssh_public_keys must be a list of paths")
    if cfg["disk_type"] == "network-ssd-nonreplicated" and cfg["disk_gb"] % NRD_CHUNK_GB != 0:
        raise ConfigError(
            f"disk_gb {cfg['disk_gb']} is not a multiple of {NRD_CHUNK_GB} "
            "(required for network-ssd-nonreplicated)"
        )
    if not cfg["_fleet"]:
        for name in runner_names(cfg):
            if not NAME_RE.fullmatch(name):
                raise ConfigError(f"bad instance name: {name}")
    return cfg


def runner_names(cfg):
    if cfg.get("names"):
        return list(cfg["names"])
    if "name" not in cfg:
        raise ConfigError("set name or names")
    name = cfg["name"]
    count = int(cfg.get("count", 1))
    if count == 1 and "start" not in cfg.get("_explicit", set()):
        return [name]
    start = int(cfg.get("start", 1))
    width = int(cfg.get("index_width", 0))
    names = []
    for index in range(start, start + count):
        suffix = f"{index:0{width}d}" if width else str(index)
        names.append(f"{name}-{suffix}")
    return names


def check_public_key(line):
    if "\n" in line or not line.startswith("ssh-") or len(line.split()) < 2:
        raise ConfigError("ssh public key must be a single ssh-* line")
    return line


def reconcile_public_key():
    raw = os.environ.get("GH_RUNNER_RECONCILE_SSH_PUB", "").strip()
    if not raw:
        return ""
    line = next((item.strip() for item in raw.splitlines() if item.strip() and not item.startswith("#")), "")
    if not line:
        return ""
    return check_public_key(line)


def public_key_lines(cfg):
    lines = []
    seen = set()
    for path in [cfg["ssh_public_key"], *cfg["ssh_public_keys"]]:
        line = check_public_key(read_public_key(path))
        if line not in seen:
            seen.add(line)
            lines.append(line)
    extra = reconcile_public_key()
    if extra and extra not in seen:
        lines.append(extra)
    return lines


def key_label(line):
    parts = line.split()
    if len(parts) >= 3:
        return f"{parts[0]} {parts[-1]}"
    return parts[0]


def render_userdata(public_keys):
    if isinstance(public_keys, str):
        public_keys = [public_keys]
    if not public_keys:
        raise ConfigError("at least one ssh public key is required")
    rendered = "\n".join("      - " + json.dumps(check_public_key(key)) for key in public_keys)
    return f"""#cloud-config
package_update: false
package_upgrade: false
users:
  - name: ubuntu
    ssh_authorized_keys:
{rendered}
  - name: runner
    groups: [sudo, adm, docker]
    shell: /bin/bash
    sudo: ALL=(ALL) NOPASSWD:SETENV:ALL
    lock_passwd: true
write_files:
  - path: /etc/sudoers.d/runner
    owner: root:root
    permissions: "0440"
    content: |
      runner ALL=(ALL) NOPASSWD:SETENV:ALL
runcmd:
  - [bash, -lc, "sysctl -w vm.max_map_count=1048576"]
  - [bash, -lc, "rdma link show | grep -q rxe_lo || rdma link add rxe_lo type rxe netdev lo || true"]
  - [touch, /var/lib/ydb-ci-host-prepared]
"""


def public_key_from_private(path):
    proc = subprocess.run(
        ["ssh-keygen", "-y", "-f", str(path)],
        check=False,
        capture_output=True,
        text=True,
    )
    line = next((item.strip() for item in (proc.stdout or "").splitlines() if item.strip()), "")
    if proc.returncode != 0 or not line:
        raise ConfigError(f"failed to read public key from {path}")
    return check_public_key(line)


def read_public_key(path):
    expanded = Path(os.path.expanduser(path))
    if not expanded.is_file():
        private = expanded.with_suffix("") if expanded.name.endswith(".pub") else None
        if private is not None and private.is_file():
            return public_key_from_private(private)
        raise ConfigError(f"ssh public key not found: {path}")
    text = expanded.read_text().strip()
    line = next((item for item in text.splitlines() if item.strip() and not item.startswith("#")), "")
    return check_public_key(line.strip())


def create_argv(cfg, name, userdata_path):
    disk = ",".join([
        f"image-family={cfg['image_family']}",
        f"image-folder-id={cfg['folder_id']}",
        f"type={cfg['disk_type']}",
        f"size={cfg['disk_gb']}",
        "auto-delete=true",
    ])
    nic = f"subnet-id={cfg['subnet_id']},ipv4-address=auto"
    if cfg.get("public_ip"):
        nic += ",nat-ip-version=ipv4"
        if cfg.get("public_address"):
            nic += f",nat-address={cfg['public_address']}"
    return [
        "yc", "--folder-id", cfg["folder_id"], "compute", "instance", "create",
        "--name", name,
        "--hostname", f"{name}.auto.internal",
        "--zone", cfg["zone"],
        "--platform", str(cfg["platform"]),
        "--service-account-id", cfg["service_account_id"],
        "--cores", str(cfg["cores"]),
        "--memory", f"{cfg['memory_gb']}G",
        "--create-boot-disk", disk,
        "--network-interface", nic,
        "--metadata", "serial-port-enable=1",
        "--metadata-from-file", f"user-data={userdata_path}",
        "--labels", "purpose=gh-runner",
        "--async",
        "--format", "json",
    ]


def ssh_argv(cfg, ip):
    return [
        "ssh",
        "-J", cfg["jump_host"],
        "-i", os.path.expanduser(cfg["ssh_identity"]),
        "-o", "IdentitiesOnly=yes",
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", "ConnectTimeout=10",
        f"{cfg['ssh_user']}@{ip}",
    ]


def run(argv, **kwargs):
    print("+", " ".join(shlex.quote(part) for part in argv), flush=True)
    return subprocess.run(argv, check=False, text=True, **kwargs)


def yc_instance(cfg, name):
    proc = run(
        ["yc", "--folder-id", cfg["folder_id"], "compute", "instance", "get", "--name", name, "--format", "json"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode != 0:
        if "not found" in (proc.stderr or "").lower():
            return None
        raise SystemExit(proc.stderr.strip() or f"yc get {name} failed")
    return json.loads(proc.stdout)


def instance_ip(info):
    address = info["network_interfaces"][0]["primary_v4_address"]["address"]
    return address


def wait_running(cfg, name, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        info = yc_instance(cfg, name)
        if info and info.get("status") == "RUNNING":
            return instance_ip(info)
        print(f"{name}: status {None if info is None else info.get('status')}", flush=True)
        time.sleep(10)
    raise SystemExit(f"{name} did not become RUNNING within {timeout}s")


def github_token(repo, kind):
    proc = run(
        ["gh", "api", "--method", "POST", f"repos/{repo}/actions/runners/{kind}", "--jq", ".token"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode != 0:
        raise SystemExit(proc.stderr.strip() or f"failed to get {kind}")
    token = proc.stdout.strip()
    if not token:
        raise SystemExit(f"empty {kind}")
    return token


def list_runners(repo):
    proc = run(
        ["gh", "api", "--paginate", f"repos/{repo}/actions/runners?per_page=100"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode != 0:
        raise SystemExit(proc.stderr.strip() or "failed to list runners")
    runners = []
    decoder = json.JSONDecoder()
    index = 0
    raw = proc.stdout
    while index < len(raw):
        while index < len(raw) and raw[index] not in "{[":
            index += 1
        if index >= len(raw):
            break
        value, index = decoder.raw_decode(raw, index)
        runners.extend(value.get("runners") or [])
    return runners


def runner_busy(repo, name):
    for runner in list_runners(repo):
        if runner.get("name") == name:
            return bool(runner.get("busy")), runner.get("status")
    return None, None


def label_name(label):
    if isinstance(label, dict):
        return str(label.get("name") or "")
    return str(label)


def label_plan(current, desired):
    """Custom labels to PUT, plus which names would be added and removed.

    GitHub-assigned labels (type read-only, or self-hosted/Linux/X64 and the
    other platform labels) stay as they are. instance:<id> is kept too: the
    register script adds it and the config does not list it.
    """
    custom = []
    preserved = []
    for label in current:
        name = label_name(label)
        if not name:
            continue
        readonly = isinstance(label, dict) and label.get("type") == "read-only"
        assigned = name in {"self-hosted", "Linux", "Windows", "macOS", "X64", "X86", "ARM", "ARM64"}
        if readonly or assigned:
            continue
        if name.startswith("instance:"):
            preserved.append(name)
        else:
            custom.append(name)
    added = [name for name in desired if name not in custom]
    removed = [name for name in custom if name not in desired]
    final = []
    for name in list(desired) + preserved:
        if name not in final:
            final.append(name)
    return final, added, removed


def managed_label_key(labels):
    """Labels that identify a pool. Platform labels and instance:<id> are not part of it."""
    names = []
    for label in labels:
        name = label_name(label)
        if not name:
            continue
        readonly = isinstance(label, dict) and label.get("type") == "read-only"
        assigned = name in {"self-hosted", "Linux", "Windows", "macOS", "X64", "X86", "ARM", "ARM64"}
        if readonly or assigned or name.startswith("instance:"):
            continue
        names.append(name)
    return tuple(sorted(names))


def pool_label(pool):
    return f"pool:{pool['_pool']}"


def effective_labels(cfg):
    labels = list(cfg["labels"])
    if cfg.get("_fleet"):
        extra = pool_label(cfg)
        if extra not in labels:
            labels.append(extra)
    return labels


def workload_key(labels):
    return tuple(name for name in managed_label_key(labels) if not str(name).startswith("pool:"))


def runner_pool_name(labels):
    for name in managed_label_key(labels):
        if str(name).startswith("pool:"):
            return str(name)[len("pool:"):]
    return None


def next_instance_name(prefix, taken):
    """Smallest prefix-N whose N is not already used, ignoring zero-padding."""
    used = set()
    head = prefix + "-"
    for name in taken:
        if not name.startswith(head):
            continue
        tail = name[len(head):]
        if tail.isdigit():
            used.add(int(tail))
    number = 1
    while number in used:
        number += 1
    candidate = f"{prefix}-{number}"
    if not NAME_RE.fullmatch(candidate):
        raise ConfigError(f"bad instance name: {candidate}")
    return candidate


def poll_pools(pools, runners):
    grouped = {tuple(sorted(pool["labels"])): [] for pool in pools}
    for runner in runners:
        key = workload_key(runner.get("labels") or [])
        if key in grouped:
            grouped[key].append(runner)
    reports = []
    for pool in pools:
        found = grouped[tuple(sorted(pool["labels"]))]
        online = [runner.get("name") for runner in found if runner.get("status") == "online"]
        offline = [runner.get("name") for runner in found if runner.get("status") != "online"]
        reports.append({
            "pool": pool.get("_pool") or "",
            "count": int(pool["count"]),
            "online": len(online),
            "offline": offline,
        })
    return reports


def poll_ok(report):
    return report["online"] == report["count"] and not report["offline"]


def format_poll(report):
    text = f"{report['pool']}: {report['online']}/{report['count']} online"
    if report["offline"]:
        text += "; offline " + ", ".join(report["offline"])
    return text


def _vm_rank(member, instance_status):
    if member["name"] not in instance_status:
        return 0
    if member.get("status") != "online":
        return 1
    if not member.get("busy"):
        return 2
    return 3


def reconcile_actions(pools, runners, instance_status):
    """Actions that bring runners in line with fleet. instance_status maps VM name to yc status."""
    members = {pool["_pool"]: [] for pool in pools}
    actions = []
    for runner in runners:
        owner = runner_pool_name(runner.get("labels") or [])
        if owner:
            if owner in members:
                members[owner].append(runner)
            else:
                actions.append({"op": "orphan", "pool": owner, "name": runner.get("name")})
            continue
        key = workload_key(runner.get("labels") or [])
        matches = [pool for pool in pools if tuple(sorted(pool["labels"])) == key]
        if len(matches) == 1:
            pool = matches[0]
            members[pool["_pool"]].append(runner)
            actions.append({
                "op": "bind",
                "pool": pool["_pool"],
                "name": runner.get("name"),
                "id": runner.get("id"),
                "current": runner.get("labels") or [],
            })
    for pool in pools:
        for runner in members[pool["_pool"]]:
            if runner_pool_name(runner.get("labels") or []) != pool["_pool"]:
                continue
            final, added, removed = label_plan(runner.get("labels") or [], effective_labels(pool))
            if added or removed:
                actions.append({
                    "op": "sync",
                    "pool": pool["_pool"],
                    "name": runner.get("name"),
                    "id": runner.get("id"),
                    "add": added,
                    "remove": removed,
                    "labels": final,
                })
    for pool in pools:
        owned = members[pool["_pool"]]
        excess = len(owned) - int(pool["count"])
        delete = []
        if excess > 0:
            ordered = sorted(owned, key=lambda member: _vm_rank(member, instance_status))
            for member in ordered:
                if len(delete) >= excess or _vm_rank(member, instance_status) == 3:
                    break
                delete.append(member)
        if excess > len(delete):
            actions.append({"op": "stuck", "pool": pool["_pool"], "count": excess - len(delete)})
        delete_ids = {member.get("id") for member in delete}
        for member in delete:
            actions.append({
                "op": "delete",
                "pool": pool["_pool"],
                "name": member.get("name"),
                "id": member.get("id"),
                "vm": member["name"] in instance_status,
            })
        kept = [member for member in owned if member.get("id") not in delete_ids]
        revive_marked = False
        future = 0
        for member in kept:
            status = instance_status.get(member["name"])
            if status is None:
                actions.append({
                    "op": "delete",
                    "pool": pool["_pool"],
                    "name": member.get("name"),
                    "id": member.get("id"),
                    "vm": False,
                })
                continue
            future += 1
            if status != "RUNNING" or member.get("status") != "online":
                actions.append({
                    "op": "start" if status != "RUNNING" else "restart",
                    "pool": pool["_pool"],
                    "name": member.get("name"),
                    "id": member.get("id"),
                    "replace": not revive_marked,
                })
                revive_marked = True
        create_n = max(0, int(pool["count"]) - future)
        if create_n:
            actions.append({"op": "create", "pool": pool["_pool"], "count": create_n})
    return actions


def format_action(action):
    pool = action.get("pool") or ""
    op = action["op"]
    if op == "orphan":
        return f"orphan pool:{pool} {action['name']}"
    if op == "bind":
        return f"{pool}: bind pool:{pool} on {action['name']}"
    if op == "sync":
        added = ", ".join(action["add"]) if action["add"] else "-"
        removed = ", ".join(action["remove"]) if action["remove"] else "-"
        return f"{pool}: sync {action['name']} add {added} remove {removed}"
    if op == "delete":
        what = "runner and VM" if action["vm"] else "runner"
        return f"{pool}: delete {what} {action['name']}"
    if op == "stuck":
        return f"{pool}: {action['count']} extra runners are busy and were left in place"
    if op == "start":
        text = f"{pool}: start VM {action['name']}"
    elif op == "restart":
        text = f"{pool}: restart service on {action['name']}"
    elif op == "create":
        return f"{pool}: create {action['count']}"
    else:
        return f"{pool}: {op}"
    if action.get("replace"):
        text += "; replace it if it stays offline"
    return text


REVIVE_WAIT = 120


def list_instances(cfg):
    proc = run(
        ["yc", "--folder-id", cfg["folder_id"], "compute", "instance", "list", "--format", "json"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode != 0:
        raise SystemExit(proc.stderr.strip() or "failed to list instances")
    payload = json.loads(proc.stdout or "[]")
    if isinstance(payload, dict):
        payload = payload.get("instances") or []
    return {item["name"]: item for item in payload if isinstance(item, dict) and item.get("name")}


def delete_github_runner(repo, runner_id):
    proc = run(
        ["gh", "api", "--method", "DELETE", f"repos/{repo}/actions/runners/{runner_id}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode != 0 and "not found" not in (proc.stderr or "").lower():
        raise SystemExit(proc.stderr.strip() or f"failed to delete runner {runner_id}")


def delete_vm(cfg, name):
    proc = run(
        ["yc", "--folder-id", cfg["folder_id"], "compute", "instance", "delete", "--name", name, "--async"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode != 0 and "not found" not in (proc.stderr or "").lower():
        raise SystemExit(proc.stderr.strip() or f"failed to delete VM {name}")


def start_vm(cfg, name, timeout):
    proc = run(
        ["yc", "--folder-id", cfg["folder_id"], "compute", "instance", "start", "--name", name],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode != 0:
        raise SystemExit(proc.stderr.strip() or f"failed to start {name}")
    return wait_running(cfg, name, timeout)


def runner_service_script():
    return """set -euo pipefail
dir=/home/runner/actions_runner
if [ ! -f "$dir/.runner" ]; then
  exit 42
fi
systemctl restart "$(cat "$dir/.service")"
"""


def wait_runner_online(repo, name, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        for runner in list_runners(repo):
            if runner.get("name") == name and runner.get("status") == "online":
                return True
        time.sleep(10)
    return False


def taken_names(runners, instances):
    names = set(instances)
    for runner in runners:
        if runner.get("name"):
            names.add(runner["name"])
    return names


def ensure_runner_up(cfg, name, timeout, allow_replace, taken, vm_was_running):
    info = yc_instance(cfg, name)
    if info is None:
        return "gone", taken
    if info.get("status") != "RUNNING":
        ip = start_vm(cfg, name, timeout)
        vm_was_running = False
    else:
        ip = instance_ip(info)
    if not vm_was_running and wait_runner_online(cfg["repo"], name, REVIVE_WAIT):
        return "online", taken
    proc = run(ssh_argv(cfg, ip) + ["sudo", "bash", "-s"], input=runner_service_script())
    if proc.returncode == 42:
        register(cfg, name, ip)
    elif proc.returncode != 0:
        print(f"{name}: service restart failed", flush=True)
    if wait_runner_online(cfg["repo"], name, REVIVE_WAIT):
        return "online", taken
    if not allow_replace:
        print(f"{name}: still offline", flush=True)
        return "offline", taken
    print(f"{name}: replacing", flush=True)
    runner_id = None
    for runner in list_runners(cfg["repo"]):
        if runner.get("name") == name:
            runner_id = runner.get("id")
            break
    if runner_id:
        delete_github_runner(cfg["repo"], runner_id)
    delete_vm(cfg, name)
    taken.discard(name)
    new_name = next_instance_name(cfg["name"], taken)
    taken.add(new_name)
    cmd_create(cfg, [new_name], False, timeout)
    return "replaced", taken


def cmd_reconcile(target, apply, timeout):
    pools = load_pools(target)
    runners = list_runners(pools[0]["repo"])
    instances = list_instances(pools[0])
    status = {name: info.get("status") for name, info in instances.items()}
    actions = reconcile_actions(pools, runners, status)
    if not actions:
        print("fleet matches", flush=True)
        return 0
    for action in actions:
        print(format_action(action), flush=True)
    if not apply:
        print("dry-run: nothing was changed", flush=True)
        return 0
    by_pool = {pool["_pool"]: pool for pool in pools}
    taken = taken_names(runners, instances)
    replaced = set()
    for action in actions:
        op = action["op"]
        pool = by_pool.get(action.get("pool"))
        if op in ("orphan", "stuck"):
            continue
        if op == "bind":
            final, added, removed = label_plan(action["current"], effective_labels(pool))
            if added or removed:
                set_runner_labels(pool["repo"], action["id"], final)
        elif op == "sync":
            set_runner_labels(pool["repo"], action["id"], action["labels"])
        elif op == "delete":
            if action.get("id"):
                delete_github_runner(pool["repo"], action["id"])
            if action.get("vm"):
                delete_vm(pool, action["name"])
            taken.discard(action["name"])
        elif op in ("start", "restart"):
            allow = bool(action.get("replace")) and action["pool"] not in replaced
            outcome, taken = ensure_runner_up(
                pool, action["name"], timeout, allow, taken, op == "restart",
            )
            if outcome == "replaced":
                replaced.add(action["pool"])
        elif op == "create":
            for _ in range(action["count"]):
                new_name = next_instance_name(pool["name"], taken)
                taken.add(new_name)
                print(f"{pool['_pool']}: creating {new_name}", flush=True)
                cmd_create(pool, [new_name], False, timeout)
    return 0


def set_runner_labels(repo, runner_id, labels):
    proc = run(
        ["gh", "api", "--method", "PUT", f"repos/{repo}/actions/runners/{runner_id}/labels", "--input", "-"],
        input=json.dumps({"labels": labels}),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode != 0:
        raise SystemExit(proc.stderr.strip() or f"failed to set labels on runner {runner_id}")


def registration_script(cfg, name):
    env = {
        "REPO_URL": f"https://github.com/{cfg['repo']}",
        "GITHUB_TOKEN": github_token(cfg["repo"], "registration-token"),
        "REMOVE_TOKEN": github_token(cfg["repo"], "remove-token"),
        "RUNNER_NAME": name,
        "RUNNER_LABELS": ",".join(effective_labels(cfg)),
        "PRESET_NAME": str(cfg.get("preset") or ""),
        "AGENT_MIRROR_URL_PREFIX": str(cfg.get("agent_mirror") or ""),
    }
    header = "\n".join(f"export {key}={shlex.quote(value)}" for key, value in env.items())
    return header + "\n" + REGISTER_SCRIPT.read_text()


def wait_prepared(cfg, ip, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        proc = run(ssh_argv(cfg, ip) + ["test", "-f", "/var/lib/ydb-ci-host-prepared"])
        if proc.returncode == 0:
            return
        time.sleep(10)
    raise SystemExit(f"{ip} did not finish cloud-init within {timeout}s")


def register(cfg, name, ip):
    busy, status = runner_busy(cfg["repo"], name)
    if busy:
        raise SystemExit(f"{name} is busy on GitHub; not re-registering")
    script = registration_script(cfg, name)
    proc = run(ssh_argv(cfg, ip) + ["sudo", "bash", "-s"], input=script)
    if proc.returncode != 0:
        raise SystemExit(f"registration failed on {name}")
    if status:
        print(f"{name}: was {status}, registration restarted", flush=True)


def create_instance(cfg, name, userdata_path):
    proc = run(create_argv(cfg, name, userdata_path), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.returncode != 0:
        raise SystemExit(proc.stderr.strip() or f"failed to create {name}")
    try:
        payload = json.loads(proc.stdout)
    except json.JSONDecodeError:
        payload = {}
    operation = payload.get("id")
    print(f"{name}: create operation {operation}", flush=True)


def selected_names(cfg, only):
    names = runner_names(cfg)
    if not only:
        return names
    missing = [name for name in only if name not in names]
    if missing:
        raise ConfigError(f"--only not in this config: {', '.join(missing)}")
    return only


def describe(cfg, names):
    print(f"repo: {cfg['repo']}")
    print(f"labels: {', '.join(effective_labels(cfg))}")
    print(
        f"vm: {cfg['platform']} {cfg['cores']} vCPU / {cfg['memory_gb']} GB / "
        f"{cfg['disk_gb']} GB {cfg['disk_type']} in {cfg['zone']}"
    )
    print("names:")
    for name in names:
        print(f"  {name}")


def authorize_script(keys):
    lines = [
        "set -euo pipefail",
        "install -d -m 700 -o ubuntu -g ubuntu /home/ubuntu/.ssh",
        "touch /home/ubuntu/.ssh/authorized_keys",
        "chown ubuntu:ubuntu /home/ubuntu/.ssh/authorized_keys",
        "chmod 600 /home/ubuntu/.ssh/authorized_keys",
        "auth=/home/ubuntu/.ssh/authorized_keys",
    ]
    for key in keys:
        quoted = shlex.quote(check_public_key(key))
        lines.append(f"grep -qxF {quoted} \"$auth\" || printf '%s\\n' {quoted} >> \"$auth\"")
    return "\n".join(lines) + "\n"


def authorize(cfg, name, ip, keys):
    proc = run(ssh_argv(cfg, ip) + ["sudo", "bash", "-s"], input=authorize_script(keys))
    if proc.returncode != 0:
        raise SystemExit(f"failed to add ssh keys on {name}")


def cmd_create(cfg, names, dry_run, timeout):
    keys = public_key_lines(cfg)
    userdata = render_userdata(keys)
    describe(cfg, names)
    if dry_run:
        print("dry-run: VM is not created, runner is not registered")
        return
    handle = tempfile.NamedTemporaryFile("w", prefix="gh-runner-userdata-", suffix=".yaml", delete=False)
    userdata_path = Path(handle.name)
    handle.write(userdata)
    handle.close()
    try:
        for name in names:
            existing = yc_instance(cfg, name)
            if existing:
                print(f"{name}: VM already exists ({existing['id']}), skipping create", flush=True)
                continue
            create_instance(cfg, name, userdata_path)
        for name in names:
            ip = wait_running(cfg, name, timeout)
            print(f"{name}: {ip}", flush=True)
            wait_prepared(cfg, ip, timeout)
            register(cfg, name, ip)
            print(f"{name}: registered", flush=True)
    finally:
        userdata_path.unlink(missing_ok=True)


def cmd_authorize(cfg, names, dry_run, timeout):
    keys = public_key_lines(cfg)
    describe(cfg, names)
    print("ssh keys for ubuntu:")
    for key in keys:
        print(f"  {key_label(key)}")
    if dry_run:
        print("dry-run: authorized_keys is not changed")
        return
    for name in names:
        info = yc_instance(cfg, name)
        if not info:
            raise SystemExit(f"{name}: VM not found")
        if info.get("status") != "RUNNING":
            ip = wait_running(cfg, name, timeout)
        else:
            ip = instance_ip(info)
        wait_prepared(cfg, ip, timeout)
        authorize(cfg, name, ip, keys)
        print(f"{name}: ssh keys updated", flush=True)


def load_pools(target):
    target = Path(target)
    files = sorted(target.glob("*.yaml")) if target.is_dir() else [target]
    if not files:
        raise ConfigError(f"no fleet files in {target}")
    pools = [load_config(path) for path in files]
    seen = {}
    for pool in pools:
        if not pool.get("_fleet"):
            raise ConfigError(f"{pool['_pool']} is not a fleet file")
        key = tuple(sorted(pool["labels"]))
        if key in seen:
            raise ConfigError(f"{seen[key]} and {pool['_pool']} use the same labels")
        seen[key] = pool["_pool"]
    return pools


def cmd_poll(target):
    pools = load_pools(target)
    runners = list_runners(pools[0]["repo"])
    reports = poll_pools(pools, runners)
    failed = False
    for report in reports:
        print(format_poll(report), flush=True)
        failed = failed or not poll_ok(report)
    return 1 if failed else 0


def cmd_labels(cfg, names, dry_run):
    by_name = {}
    duplicates = []
    for runner in list_runners(cfg["repo"]):
        name = runner.get("name")
        if name in by_name:
            duplicates.append(name)
        by_name[name] = runner
    if duplicates:
        raise SystemExit(f"duplicate runner names: {', '.join(sorted(set(duplicates)))}")
    describe(cfg, names)
    for name in names:
        runner = by_name.get(name)
        if not runner:
            raise SystemExit(f"{name}: runner not found on GitHub")
        final, added, removed = label_plan(runner.get("labels") or [], cfg["labels"])
        if not added and not removed:
            print(f"{name}: labels already match", flush=True)
            continue
        print(
            f"{name}: add {', '.join(added) if added else '-'} "
            f"remove {', '.join(removed) if removed else '-'}",
            flush=True,
        )
        if dry_run:
            continue
        set_runner_labels(cfg["repo"], runner["id"], final)
        print(f"{name}: labels updated", flush=True)


def cmd_register(cfg, names, dry_run, timeout):
    describe(cfg, names)
    if dry_run:
        print("dry-run: runner is not re-registered")
        return
    for name in names:
        info = yc_instance(cfg, name)
        if not info:
            raise SystemExit(f"{name}: VM not found")
        if info.get("status") != "RUNNING":
            ip = wait_running(cfg, name, timeout)
        else:
            ip = instance_ip(info)
        wait_prepared(cfg, ip, timeout)
        register(cfg, name, ip)
        print(f"{name}: registered", flush=True)


def self_test():
    sample = """
# comment
name: tiny-worker-2026-10
count: 2
start: 8
index_width: 2
platform: standard-v3
cores: 4
memory_gb: 8
disk_gb: 186
labels:
  - tiny-worker
  - auto-provisioned
"""
    cfg = load_config_text(sample)
    assert runner_names(cfg) == ["tiny-worker-2026-10-08", "tiny-worker-2026-10-09"]
    os.environ["YC_FOLDER_ID"] = "folder-from-env"
    try:
        from_env = load_config_text(sample)
        assert from_env["folder_id"] == "folder-from-env"
        pinned = load_config_text(sample + "folder_id: pinned-folder\n")
        assert pinned["folder_id"] == "pinned-folder"
    finally:
        os.environ.pop("YC_FOLDER_ID", None)
    plain = load_config_text(sample.replace("count: 2\nstart: 8\nindex_width: 2\n", ""))
    assert runner_names(plain) == ["tiny-worker-2026-10"]
    listed = load_config_text(
        "names:\n  - analytic-node-1\n"
        "platform: standard-v4a\ncores: 8\nmemory_gb: 64\ndisk_gb: 186\n"
        "labels:\n  - auto-provisioned\n"
    )
    assert runner_names(listed) == ["analytic-node-1"]
    try:
        load_config_text(sample.replace("disk_gb: 186", "disk_gb: 200"))
    except ConfigError as exc:
        assert "multiple of 93" in str(exc)
    else:
        raise AssertionError("expected disk alignment error")
    first = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAItest key"
    second = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIother key"
    userdata = render_userdata([first, second])
    assert first in userdata and second in userdata
    assert "/var/lib/ydb-ci-host-prepared" in userdata
    assert "GITHUB_TOKEN" not in userdata
    script = authorize_script([first, second])
    assert script.count("grep -qxF") == 2
    assert ">>" in script
    assert ">" not in script.replace(">>", "")
    keydir = Path(tempfile.mkdtemp(prefix="gh-runner-keys-"))
    try:
        primary = keydir / "primary.pub"
        extra = keydir / "extra.pub"
        primary.write_text(first + "\n")
        extra.write_text(second + "\n")
        keyed = load_config_text(
            "name: tiny\nplatform: standard-v4a\ncores: 4\nmemory_gb: 8\ndisk_gb: 186\n"
            "labels:\n  - tiny-worker\n"
            f"ssh_public_key: {primary}\n"
            "ssh_public_keys:\n"
            f"  - {extra}\n"
            f"  - {primary}\n"
        )
        assert public_key_lines(keyed) == [first, second]
        os.environ["GH_RUNNER_RECONCILE_SSH_PUB"] = second
        try:
            assert public_key_lines(keyed) == [first, second]
            os.environ["GH_RUNNER_RECONCILE_SSH_PUB"] = first.replace("test", "workflow")
            workflow_key = first.replace("test", "workflow")
            assert public_key_lines(keyed) == [first, second, workflow_key]
        finally:
            os.environ.pop("GH_RUNNER_RECONCILE_SSH_PUB", None)
        derived = keydir / "derived"
        subprocess.run(
            ["ssh-keygen", "-t", "ed25519", "-f", str(derived), "-N", "", "-C", "derived-test"],
            check=True,
            capture_output=True,
            text=True,
        )
        expected = (derived.with_suffix(".pub")).read_text().strip()
        derived.with_suffix(".pub").unlink()
        from_private = load_config_text(
            "name: tiny\nplatform: standard-v4a\ncores: 4\nmemory_gb: 8\ndisk_gb: 186\n"
            "labels:\n  - tiny-worker\n"
            f"ssh_public_key: {derived}.pub\n"
        )
        assert public_key_lines(from_private) == [expected]
        derived.unlink()
    finally:
        primary.unlink(missing_ok=True)
        extra.unlink(missing_ok=True)
        for name in ("derived", "derived.pub"):
            (keydir / name).unlink(missing_ok=True)
        keydir.rmdir()
    try:
        render_userdata(["not-a-key"])
    except ConfigError:
        pass
    else:
        raise AssertionError("expected a public key error")
    current = [
        {"name": "self-hosted", "type": "read-only"},
        {"name": "Linux", "type": "read-only"},
        {"name": "X64", "type": "read-only"},
        {"name": "auto-provisioned", "type": "custom"},
        {"name": "build-preset-relwithdebinfo", "type": "custom"},
        {"name": "build-preset-release-asan", "type": "custom"},
        {"name": "instance:abc", "type": "custom"},
    ]
    desired = ["auto-provisioned", "postcommit", "build-preset-relwithdebinfo", "build-preset-release-asan"]
    final, added, removed = label_plan(current, desired)
    assert added == ["postcommit"]
    assert removed == []
    assert "instance:abc" in final
    assert "self-hosted" not in final
    same, added, removed = label_plan(current + [{"name": "postcommit", "type": "custom"}], desired)
    assert added == [] and removed == [] and same == final
    extra, added, removed = label_plan(current + [{"name": "old-label", "type": "custom"}], desired)
    assert removed == ["old-label"] and "old-label" not in extra
    reports = poll_pools(
        [{
            "_pool": "tiny",
            "count": 2,
            "labels": ["tiny-worker"],
            "_explicit": set(),
            "_fleet": True,
        }],
        [
            {"name": "tiny-a", "status": "online", "labels": [
                {"name": "self-hosted", "type": "read-only"},
                {"name": "tiny-worker", "type": "custom"},
                {"name": "instance:abc", "type": "custom"},
            ]},
            {"name": "tiny-b", "status": "offline", "labels": [{"name": "tiny-worker", "type": "custom"}]},
        ],
    )
    assert reports[0]["online"] == 1
    assert reports[0]["offline"] == ["tiny-b"]
    assert not poll_ok(reports[0])
    taken = {"static-relwithdebinfo-d", "static-relwithdebinfo-d-01", "static-relwithdebinfo-d-2"}
    assert next_instance_name("static-relwithdebinfo-d", taken) == "static-relwithdebinfo-d-3"
    tiny_pool = {
        "_pool": "tiny", "_fleet": True, "count": 1, "name": "tiny-worker",
        "labels": ["tiny-worker"], "repo": "ydb-platform/ydb",
    }
    online = {
        "id": 1, "name": "tiny-worker-1", "status": "online", "busy": True,
        "labels": [
            {"name": "tiny-worker", "type": "custom"},
            {"name": "pool:tiny", "type": "custom"},
            {"name": "instance:abc", "type": "custom"},
        ],
    }
    offline = {
        "id": 2, "name": "tiny-worker-8", "status": "offline", "busy": False,
        "labels": [
            {"name": "tiny-worker", "type": "custom"},
            {"name": "pool:tiny", "type": "custom"},
        ],
    }
    plain = {
        "id": 3, "name": "tiny-worker-9", "status": "online", "busy": False,
        "labels": [{"name": "tiny-worker", "type": "custom"}, {"name": "instance:z", "type": "custom"}],
    }
    actions = reconcile_actions(
        [tiny_pool],
        [online, offline, plain],
        {"tiny-worker-1": "RUNNING", "tiny-worker-8": "RUNNING", "tiny-worker-9": "RUNNING"},
    )
    ops = [(item["op"], item.get("name")) for item in actions]
    assert ("bind", "tiny-worker-9") in ops
    assert ("delete", "tiny-worker-8") in ops
    assert all(item["op"] != "create" for item in actions)
    short = reconcile_actions(
        [{**tiny_pool, "count": 2}],
        [online],
        {"tiny-worker-1": "RUNNING"},
    )
    assert any(item["op"] == "create" and item["count"] == 1 for item in short)
    stopped = reconcile_actions(
        [{**tiny_pool, "count": 1}],
        [online],
        {"tiny-worker-1": "STOPPED"},
    )
    assert any(item["op"] == "start" and item["replace"] for item in stopped)
    pools = load_pools(HERE / "fleet")
    assert all(not pool.get("names") and pool.get("name") for pool in pools)
    assert {pool["_pool"] for pool in pools} == {
        "analytic",
        "tiny",
        "static-relwithdebinfo-release",
        "static-relwithdebinfo-msan-tsan",
        "static-relwithdebinfo-asan",
        "static-relwithdebinfo-asan-postcommit",
    }
    assert sum(pool["count"] for pool in pools) == 49
    argv = create_argv(cfg, "tiny-worker-2026-10-08", "/tmp/user-data")
    assert "--async" in argv
    assert "purpose=gh-runner" in argv
    assert "size=186" in " ".join(argv)
    print("self-test ok")


def load_config_text(text):
    path = Path(os.environ.get("TMPDIR", "/tmp")) / "gh-runner-provision-self-test.yaml"
    path.write_text(text)
    try:
        return load_config(path)
    finally:
        path.unlink(missing_ok=True)


def command_names(cfg, only, names):
    if cfg.get("_fleet"):
        if not names:
            raise ConfigError("fleet file needs --name")
        for name in names:
            if not NAME_RE.fullmatch(name):
                raise ConfigError(f"bad instance name: {name}")
        return list(names)
    return selected_names(cfg, only or names)


def main(argv):
    parser = argparse.ArgumentParser(description="Create or re-register a GitHub Actions runner VM from a config file.")
    parser.add_argument(
        "command",
        choices=("create", "register", "labels", "poll", "reconcile", "authorize", "self-test"),
    )
    parser.add_argument("config", nargs="?")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true", help="reconcile performs the plan; default prints it")
    parser.add_argument("--only", action="append", default=[], help="instance name from this config; repeatable")
    parser.add_argument("--name", action="append", default=[], help="VM name; required when the config is a fleet file")
    parser.add_argument("--timeout", type=int, default=900, help="seconds to wait for the VM and cloud-init")
    args = parser.parse_args(argv)
    if args.command == "self-test":
        self_test()
        return
    if args.command in ("poll", "reconcile"):
        target = args.config or (HERE / "fleet")
        try:
            if args.command == "poll":
                raise SystemExit(cmd_poll(target))
            raise SystemExit(cmd_reconcile(target, args.apply, args.timeout))
        except ConfigError as exc:
            raise SystemExit(f"config: {exc}") from exc
    if not args.config:
        parser.error("config file is required")
    try:
        cfg = load_config(args.config)
        names = command_names(cfg, args.only, args.name)
    except ConfigError as exc:
        raise SystemExit(f"config: {exc}") from exc
    if args.command == "create":
        cmd_create(cfg, names, args.dry_run, args.timeout)
    elif args.command == "authorize":
        cmd_authorize(cfg, names, args.dry_run, args.timeout)
    elif args.command == "labels":
        cmd_labels(cfg, names, args.dry_run)
    else:
        cmd_register(cfg, names, args.dry_run, args.timeout)


if __name__ == "__main__":
    main(sys.argv[1:])
