"""Explicit local process inventory and Linux pidfd-based YDB process control.

No harness objects or guessed service names. Restart recipes come from operator
configuration, never from an HTTP request or a partially observed command line.
"""
from __future__ import annotations

import os
from pathlib import Path
import signal
import subprocess
import threading
import time
from dataclasses import dataclass

import yaml


def ic_port(argv):
    ports = []
    for i, arg in enumerate(argv):
        if arg == "--ic-port" and i + 1 < len(argv):
            ports.append(int(argv[i + 1]))
        elif arg.startswith("--ic-port="):
            ports.append(int(arg.split("=", 1)[1]))
    return ports[0] if len(ports) == 1 else None


@dataclass(frozen=True)
class ProcessSpec:
    host: str
    kind: str
    node_id: int | None
    slot_idx: int | None
    ic_port: int
    executable: str
    rack: str
    datacenter: str
    bridge_pile_id: int | None
    uid: int
    restart: dict


def load_process_specs(path):
    with open(path) as stream:
        rows = yaml.safe_load(stream).get("processes", [])
    specs = []
    for row in rows:
        restart = dict(row["restart"])
        executable = str(Path(row["executable"]).resolve(strict=True))
        kind = row["kind"]
        port = int(row["ic_port"])
        if kind not in ("node", "slot") or not 0 < port < 65536:
            raise ValueError("Local process needs a node/slot kind and valid ic_port")
        node_id = int(row["node_id"]) if row.get("node_id") is not None else None
        slot_idx = int(row["slot_idx"]) if row.get("slot_idx") is not None else None
        if (kind == "node" and node_id is None) or (kind == "slot" and slot_idx is None):
            raise ValueError("Local node_id / slot_idx is required")
        unit = restart.get("unit")
        if unit:
            if not isinstance(unit, str) or not unit.endswith(".service") or any(
                c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789@_.:-" for c in unit
            ):
                raise ValueError("Invalid restart unit")
        else:
            argv = restart.get("argv")
            if (not isinstance(argv, list) or not argv or not all(isinstance(a, str) for a in argv)
                    or str(Path(argv[0]).resolve()) != executable or ic_port(argv) != port):
                raise ValueError("Restart argv must name the same executable and exact ic_port")
            if not Path(restart.get("cwd", "")).is_absolute() or not Path(restart["cwd"]).is_dir():
                raise ValueError("Restart cwd must be explicit and absolute")
            environment = restart.get("environment", {})
            if not isinstance(environment, dict) or any(
                not isinstance(k, str) or not k or "=" in k or "\0" in k
                or not isinstance(v, str) or "\0" in v for k, v in environment.items()
            ):
                raise ValueError("Restart environment must contain valid string keys/values")
        specs.append(ProcessSpec(
            host=row["host"], kind=kind, node_id=node_id, slot_idx=slot_idx,
            ic_port=port, executable=executable, rack=str(row["rack"]),
            datacenter=str(row["datacenter"]), bridge_pile_id=row.get("bridge_pile_id"),
            uid=int(row.get("uid", os.geteuid())), restart=restart,
        ))
    if not specs or len({s.ic_port for s in specs}) != len(specs):
        raise ValueError("Local process ports must be non-empty and unique")
    for kind in ("node", "slot"):
        ids = [s.node_id if kind == "node" else s.slot_idx for s in specs if s.kind == kind]
        if len(ids) != len(set(ids)):
            raise ValueError("Duplicate local resource identity")
    if len({s.host for s in specs}) != 1:
        raise ValueError("Local process config describes exactly one physical agent host")
    return specs


@dataclass(frozen=True)
class ProcessRef:
    pid: int
    starttime: int
    state: str


class ProcessController:
    def __init__(self, specs, *, proc_root="/proc", run=subprocess.run, timeout=15):
        self.specs = tuple(specs)
        self._proc = Path(proc_root)
        self._run = run
        self._timeout = timeout
        self._lock = threading.RLock()
        self._active = {}

    def spec_for(self, target):
        found = [s for s in self.specs if s.host == target.host and s.kind == target.kind.value
                 and (target.ic_port is None or target.ic_port == s.ic_port)
                 and (target.kind.value != "node" or target.node_id is None or target.node_id == s.node_id)
                 and (target.kind.value != "slot" or target.slot_idx is None or target.slot_idx == s.slot_idx)]
        if len(found) != 1:
            raise ValueError("Target does not resolve to exactly one configured local process")
        return found[0]

    def _read(self, pid, spec):
        path = self._proc / str(pid)
        stat = (path / "stat").read_text().rsplit(")", 1)[1].split()
        if path.stat().st_uid != spec.uid:
            return None
        expected, actual = os.stat(spec.executable), (path / "exe").stat()
        if (expected.st_dev, expected.st_ino) != (actual.st_dev, actual.st_ino):
            return None
        argv = [os.fsdecode(a) for a in (path / "cmdline").read_bytes().split(b"\0") if a]
        if ic_port(argv) != spec.ic_port or stat[0] == "Z":
            return None
        return ProcessRef(int(pid), int(stat[19]), stat[0])

    def discover(self, spec):
        found = []
        for path in self._proc.iterdir():
            if not path.name.isdigit():
                continue
            try:
                ref = self._read(int(path.name), spec)
                if ref is not None:
                    found.append(ref)
            except (FileNotFoundError, PermissionError, ProcessLookupError):
                continue
        if len(found) > 1:
            raise RuntimeError("Ambiguous local process: multiple exact executable/ic-port matches")
        return found[0] if found else None

    def _signal(self, spec, ref, sig):
        # pidfd binds the signal to this process even if its numeric PID is reused.
        fd = os.pidfd_open(ref.pid)
        try:
            current = self._read(ref.pid, spec)
            if current is None or current.starttime != ref.starttime:
                raise RuntimeError("Process identity changed before signal")
            signal.pidfd_send_signal(fd, sig)
        finally:
            os.close(fd)

    def _wait(self, predicate, message):
        deadline = time.monotonic() + self._timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.05)
        raise TimeoutError(message)

    def _same(self, spec, ref):
        try:
            current = self._read(ref.pid, spec)
            return current is not None and current.starttime == ref.starttime
        except (FileNotFoundError, ProcessLookupError):
            return False

    def inject(self, target, mode):
        with self._lock:
            spec = self.spec_for(target)
            if spec.ic_port in self._active:
                raise RuntimeError("Local process already has an active fault")
            ref = self.discover(spec)
            if ref is None or ref.state in ("T", "t"):
                raise RuntimeError("Local process missing or already stopped")
            if mode not in ("kill", "stop", "suspend"):
                raise ValueError("Unknown process action")
            if not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal"):
                raise RuntimeError("Local process control requires Linux pidfd support")
            if mode == "stop" and spec.restart.get("unit"):
                unit = spec.restart["unit"]
                main = self._run(["systemctl", "show", unit, "--property=MainPID", "--value"],
                                 check=True, capture_output=True, text=True, timeout=self._timeout)
                if int(main.stdout.strip()) != ref.pid:
                    raise RuntimeError("Configured service does not exclusively own the target process")
            # Keep recovery ownership even if the command succeeds and reporting fails.
            self._active[spec.ic_port] = (spec, ref, mode)
            if mode == "stop" and spec.restart.get("unit"):
                self._run(["systemctl", "stop", spec.restart["unit"]], check=True, timeout=self._timeout)
            else:
                self._signal(spec, ref, {"kill": signal.SIGKILL, "stop": signal.SIGTERM,
                                         "suspend": signal.SIGSTOP}[mode])
            if mode == "suspend":
                self._wait(lambda: self._same(spec, ref) and self._read(ref.pid, spec).state in ("T", "t"),
                           "Process did not suspend")
            else:
                self._wait(lambda: not self._same(spec, ref), "Process did not exit")
                if mode == "kill":
                    self.restore(target)

    def _restart(self, spec):
        if spec.restart.get("unit"):
            cmd = ["systemctl", "start", spec.restart["unit"]]
        else:
            # A separate transient service survives systemctl stop nemesis. A
            # detached Popen alone would still belong to the Nemesis cgroup.
            cmd = ["systemd-run", "--quiet", "--collect", "--service-type=exec",
                   "--property=Restart=no", f"--property=User={spec.uid}",
                   f"--property=WorkingDirectory={spec.restart['cwd']}"]
            for key, value in spec.restart.get("environment", {}).items():
                if not isinstance(key, str) or not key or "=" in key or "\0" in key:
                    raise ValueError("Invalid restart environment key")
                cmd.append(f"--setenv={key}={value}")
            cmd.extend(["--", *spec.restart["argv"]])
        self._run(cmd, check=True, timeout=self._timeout)

    def restore(self, target):
        with self._lock:
            spec = self.spec_for(target)
            active = self._active.get(spec.ic_port)
            if active is None:
                return
            _, ref, mode = active
            current = self.discover(spec)
            if mode == "suspend" and self._same(spec, ref):
                self._signal(spec, ref, signal.SIGCONT)
            elif current is None:
                self._restart(spec)
            elif mode == "stop" and self._same(spec, ref):
                raise RuntimeError("Original process never stopped; refusing duplicate restart")
            self._wait(lambda: (p := self.discover(spec)) is not None and p.state not in ("T", "t"),
                       "Restored process did not appear or is still stopped")
            del self._active[spec.ic_port]


class LocalProcessRunner:
    def __init__(self, controller, mode):
        self.controller = controller
        self.mode = mode

    def inject_fault(self, payload):
        from ydb.tests.stability.nemesis.internal.nemesis.runners.target_payload import target_from_payload
        self.controller.inject(target_from_payload(payload), self.mode)

    def extract_fault(self, payload):
        from ydb.tests.stability.nemesis.internal.nemesis.runners.target_payload import target_from_payload
        self.controller.restore(target_from_payload(payload))

    def on_failed_inject_fault(self):
        pass

    def on_failed_extract_fault(self):
        pass


def local_definitions(definitions, controller):
    # Host/network/disk/group faults are not safe substitutes for a local process fault.
    modes = {"KillNodeNemesis": "kill", "KillNodeDaemonNemesis": "kill",
             "KillSlotDaemonNemesis": "kill", "StopStartNodeNemesis": "stop",
             "SuspendNodeNemesis": "suspend"}
    return {name: {**definitions[name], "runner": LocalProcessRunner(controller, mode)}
            for name, mode in modes.items() if name in definitions}
