"""`ydbd_mock cluster ...`: a process manager plus a client of the control API.

Run directory (YDBD_MOCK_RUN_DIR, default ~/ydbd_mock_work/run):

    topology.yaml       written by `up`
    endpoint_map.json   label -> 127.0.0.1:port, for the CLI shim
    cluster.json        how to respawn an instance
    macros.json         faults set by macros, cleared by `heal`
    pids/<pile>.pid
    state/<pile>/replica.json
    logs/<pile>.log             instance stdout and stderr
    logs/<pile>.events.jsonl    event log, survives a kill
"""
import argparse
import asyncio
import hashlib
import heapq
import ipaddress
import json
import os
import signal
import subprocess
import sys
import time
import typing
import urllib.error
import urllib.request
import uuid

from aiohttp import web

from ydb.apps.bridge_skipper.ydbd_mock.lib.topology import Topology, TopologyError

DEFAULT_WORK = os.path.join(os.path.expanduser("~"), "ydbd_mock_work")


class SupervisorError(Exception):
    pass


class PartialMutationError(SupervisorError):
    """The command has started changing the cluster; an error is not proof of rollback."""

    def __init__(self, message: str, report: dict) -> None:
        super().__init__(message)
        self.report = report


class _MutationReport:
    def __init__(self, action: str, piles: typing.Sequence[str]) -> None:
        self.data = {"ok": False, "action": action, "stage": "starting", "piles": list(piles),
                     "attempted": {}, "results": {}, "errors": {}, "uncertain": True}

    def step(self, stage: str, target: str, call):
        if not self.data["errors"]:
            self.data["stage"] = stage
        self.data["attempted"].setdefault(stage, []).append(target)
        try:
            result = call()
        except Exception as e:
            self.data["errors"].setdefault(stage, {})[target] = str(e)
            raise PartialMutationError(str(e), self.data) from e
        self.data["results"].setdefault(stage, {})[target] = result
        return result


def default_run_dir() -> str:
    return os.environ.get("YDBD_MOCK_RUN_DIR") or os.path.join(
        os.environ.get("YDBD_MOCK_WORK") or DEFAULT_WORK, "run")


def argv_prefix() -> typing.List[str]:
    exe = os.environ.get("YDBD_MOCK_EXE")
    if exe:
        return [exe]
    if getattr(sys, "is_standalone_binary", False):
        return [sys.executable]
    return [sys.executable, "-m", "ydb.apps.bridge_skipper.ydbd_mock"]


def pid_alive(pid: int) -> bool:
    try:
        done, _ = os.waitpid(pid, os.WNOHANG)   # reaps our own zombies
        if done == pid:
            return False
    except ChildProcessError:
        pass
    except OSError:
        pass
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def command_line(pid: int) -> str:
    try:
        done = subprocess.run(["ps", "-ww", "-o", "command=", "-p", str(pid)],
                              capture_output=True, text=True, timeout=5)
    except (OSError, subprocess.SubprocessError):
        return ""
    return done.stdout.strip()


class Cluster:
    """A run directory and the cluster that lives in it."""

    def __init__(self, run_dir: typing.Optional[str] = None) -> None:
        self.run = os.path.abspath(run_dir or default_run_dir())
        self._topo: typing.Optional[Topology] = None

    # ----- paths -----

    def path(self, *parts: str) -> str:
        return os.path.join(self.run, *parts)

    @property
    def topology(self) -> Topology:
        if self._topo is None:
            if not os.path.exists(self.path("topology.yaml")):
                raise SupervisorError("no cluster in %s: run `cluster up` first" % self.run)
            self._topo = Topology.load(self.path("topology.yaml"))
        return self._topo

    def piles(self) -> typing.List[str]:
        return list(self.topology.pile_names)

    def pile(self, name: str) -> str:
        if name not in self.topology.pile_names:
            raise SupervisorError("unknown pile %r, known: %s" % (name, ", ".join(self.topology.pile_names)))
        return name

    # ----- processes -----

    def pid(self, pile: str) -> typing.Optional[int]:
        try:
            with open(self.path("pids", "%s.pid" % pile)) as f:
                return int(f.read().strip())
        except (OSError, ValueError):
            return None

    def alive(self, pile: str) -> bool:
        """The pid file names a running process that really is this pile of this cluster.

        A pid file outlives its process (a reboot, a SIGKILL of the whole group): the pid may belong to
        anybody by now, and `down`, `kill` or `stop` must never signal a stranger.
        """
        pid = self.pid(pile)
        if pid is None or not pid_alive(pid):
            return False
        argv = command_line(pid) + " "
        prefix = "--topology %s --pile" % self.path("topology.yaml")
        return (prefix + "=%s " % pile) in argv or (bool(pile) and (prefix + " %s " % pile) in argv)

    def _config(self) -> dict:
        with open(self.path("cluster.json")) as f:
            return json.load(f)

    def spawn(self, pile: str, truncate_logs: bool = False) -> subprocess.Popen:
        config = self._config()
        os.makedirs(self.path("logs"), exist_ok=True)
        os.makedirs(self.path("pids"), exist_ok=True)
        mode = "w" if truncate_logs else "a"
        log = open(self.path("logs", "%s.log" % pile), mode)
        argv = list(config["argv_prefix"]) + [
            "run",
            "--topology", self.path("topology.yaml"),
            "--pile=%s" % pile,
            "--state-dir", self.path("state", pile),
            "--log-events", self.path("logs", "%s.events.jsonl" % pile),
            "--log-level", config.get("log_level", "INFO"),
        ]
        env = dict(os.environ)
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        proc = subprocess.Popen(argv, stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                                start_new_session=True, env=env)
        log.close()
        with open(self.path("pids", "%s.pid" % pile), "w") as f:
            f.write("%d\n" % proc.pid)
        return proc

    def wait_responsive(self, piles: typing.Sequence[str], procs: typing.Dict[str, subprocess.Popen],
                        timeout: float = 20.0) -> None:
        deadline = time.monotonic() + timeout
        pending = list(piles)
        while pending and time.monotonic() < deadline:
            for pile in list(pending):
                proc = procs.get(pile)
                if proc is not None and proc.poll() is not None:
                    raise SupervisorError("instance %s exited with code %s:\n%s" % (
                        pile, proc.returncode, self.log_tail(pile)))
                try:
                    state = self.request(pile, "GET", "/state", timeout=1.0)
                    # an answer from somebody else's process on our port does not count
                    if proc is None or state.get("pid") == proc.pid:
                        pending.remove(pile)
                except SupervisorError:
                    pass
            if pending:
                time.sleep(0.1)
        if pending:
            raise SupervisorError("instances not responding after %.0fs: %s\n%s" % (
                timeout, ", ".join(pending), "\n".join(self.log_tail(p) for p in pending)))

    def log_tail(self, pile: str, lines: int = 20) -> str:
        try:
            with open(self.path("logs", "%s.log" % pile)) as f:
                return "".join(f.readlines()[-lines:])
        except OSError:
            return "(no log)"

    # ----- control API client -----

    def request(self, pile: str, method: str, path: str, body: typing.Optional[dict] = None,
                timeout: float = 5.0) -> dict:
        url = self.topology.control_url(self.pile(pile)) + path
        data = None
        headers = {}
        if body is not None:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as response:
                return json.loads(response.read().decode())
        except urllib.error.HTTPError as e:
            try:
                detail = json.loads(e.read().decode()).get("error", "")
            except (ValueError, OSError):
                detail = ""
            raise SupervisorError("%s %s on %s: HTTP %d %s" % (method, path, pile, e.code, detail))
        except (urllib.error.URLError, OSError, TimeoutError) as e:
            raise SupervisorError("%s %s on %s: %s" % (method, path, pile, getattr(e, "reason", e)))

    def live_piles(self) -> typing.List[str]:
        return [p for p in self.piles() if self.alive(p)]

    def manifest(self, supervisor_id: typing.Optional[str] = None) -> dict:
        """Public dial inventory: callers must keep logical identity separate from transport addresses."""
        topo = self.topology
        nodes = [{"node_id": node.node_id, "label": node.label, "pile": node.pile_name,
                  "dial_endpoint": node.endpoint, "tls": bool(topo.tls)} for node in topo.all_nodes()]
        inventory = {"cluster_name": topo.cluster_name, "nodes": nodes,
                     "piles": [{"name": p.name, "control_endpoint": p.control_endpoint} for p in topo.piles]}
        digest = hashlib.sha256(json.dumps(inventory, sort_keys=True).encode()).hexdigest()
        try:
            config = self._config()
        except (OSError, ValueError):
            config = {}
        cluster_id = config.get("cluster_id") or uuid.uuid5(uuid.NAMESPACE_URL, self.run + digest).hex
        return {"schema_version": 1, "kind": "ydbd_mock", "cluster_id": cluster_id,
                "supervisor_id": supervisor_id, "topology_sha256": digest,
                "endpoint_map": topo.endpoint_map(), **inventory}

    def aggregate_state(self) -> dict:
        out = {}
        for pile in self.piles():
            row = {"pid": self.pid(pile), "alive": self.alive(pile), "responsive": False}
            if row["alive"]:
                try:
                    row["state"] = self.request(pile, "GET", "/state", timeout=1.0)
                    row["responsive"] = True
                except SupervisorError as e:
                    row["error"] = str(e)
            out[pile] = row
        return {"schema_version": 1, "cluster_id": self.manifest()["cluster_id"], "instances": out}

    # ----- macros bookkeeping -----

    def _macros(self) -> typing.List[dict]:
        try:
            with open(self.path("macros.json")) as f:
                return json.load(f)
        except (OSError, ValueError):
            return []

    def _save_macros(self, macros: typing.List[dict]) -> None:
        with open(self.path("macros.json"), "w") as f:
            json.dump(macros, f)

    def record_macro(self, entry: dict) -> None:
        macros = self._macros()
        if entry not in macros:
            macros.append(entry)
        self._save_macros(macros)


# ----- commands -----

def _print(obj) -> None:
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def cmd_up(cluster: Cluster, args) -> int:
    run = cluster.run
    if args.topology:
        topology = Topology.load(args.topology)
    else:
        piles = args.piles.split(",")
        if any(not name for name in piles):
            raise SupervisorError(
                '--piles must contain nonempty names separated by commas; '
                'for an explicit empty pile name, use --topology with YAML piles: [named, ""]')
        topology = Topology(
            piles, args.nodes_per_pile, cluster_name=args.cluster_name,
            control_base_port=args.control_base_port, grpc_base_port=args.grpc_base_port)
    from ydb.apps.bridge_skipper.ydbd_mock.lib import instance
    instance.apply_overrides(topology, args.set or [])
    piles_to_check = set(topology.pile_names)
    if os.path.exists(cluster.path("topology.yaml")):
        piles_to_check.update(cluster.piles())
    for name in sorted(piles_to_check):
        if cluster.alive(name):
            raise SupervisorError("pile %s is already running (pid %s); run `cluster down` first" % (
                name, cluster.pid(name)))
    os.makedirs(run, exist_ok=True)
    for sub in ("pids", "logs", "state"):
        os.makedirs(cluster.path(sub), exist_ok=True)
    if args.fresh:
        for name in topology.pile_names:
            for path in (cluster.path("state", name, "replica.json"),
                         cluster.path("logs", "%s.events.jsonl" % name)):
                if os.path.exists(path):
                    os.remove(path)
    topology.save(cluster.path("topology.yaml"))
    with open(cluster.path("endpoint_map.json"), "w") as f:
        json.dump(topology.endpoint_map(), f, indent=2, sort_keys=True)
    with open(cluster.path("cluster.json"), "w") as f:
        json.dump({"argv_prefix": argv_prefix(), "log_level": args.log_level,
                   "cluster_id": uuid.uuid4().hex}, f)
    cluster._save_macros([])
    cluster._topo = None
    with open(cluster.path("endpoint_manifest.json"), "w") as f:
        json.dump(cluster.manifest(), f, indent=2, sort_keys=True)
    for w in topology.warnings:
        sys.stderr.write("warning: %s\n" % w)

    procs = {}
    try:
        for name in topology.pile_names:
            procs[name] = cluster.spawn(name, truncate_logs=True)
        cluster.wait_responsive(topology.pile_names, procs)
    except SupervisorError:
        for name, proc in procs.items():
            if proc.poll() is None:
                proc.kill()
            proc.wait()
            _remove_pid_file(cluster, name)
        raise
    _print({
        "ready": True,
        "cluster": topology.cluster_name,
        "instances": [{
            "pile": p.name, "pid": procs[p.name].pid, "control": p.control_endpoint,
            "nodes": [{"id": n.node_id, "label": n.label, "endpoint": n.endpoint} for n in p.nodes],
        } for p in topology.piles],
    })
    return 0


def _remove_pid_file(cluster: Cluster, pile: str) -> None:
    try:
        os.remove(cluster.path("pids", "%s.pid" % pile))
    except OSError:
        pass


def cmd_down(cluster: Cluster, args) -> int:
    try:
        piles = cluster.piles()
    except SupervisorError:
        return 0
    ours = {p: cluster.pid(p) for p in piles if cluster.alive(p)}   # never a stranger behind a stale pid file
    for pid in ours.values():
        _signal(pid, signal.SIGCONT)
        _signal(pid, signal.SIGTERM)
    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline and any(pid_alive(pid) for pid in ours.values()):
        time.sleep(0.05)
    for pid in ours.values():
        if pid_alive(pid):
            _signal(pid, signal.SIGKILL)
    for pile in piles:
        _remove_pid_file(cluster, pile)
    return 0


def _signal(pid: int, sig: int) -> None:
    try:
        os.kill(pid, sig)
    except ProcessLookupError:
        pass


def _vector(state: dict) -> str:
    return " ".join("%s=%s" % (p["name"], p["public"]) for p in state["piles"])


def cmd_status(cluster: Cluster, args) -> int:
    rows = []
    for pile in cluster.piles():
        pid = cluster.pid(pile)
        row = {"pile": pile, "pid": pid, "alive": cluster.alive(pile)}
        if row["alive"]:
            try:
                state = cluster.request(pile, "GET", "/state", timeout=1.0)
                row.update({"responsive": True, "generation": state["generation"], "ready": state["ready"],
                            "detached": state["detached"], "vector": _vector(state)})
            except SupervisorError:
                row["responsive"] = False
        rows.append(row)
    if args.json:
        _print(rows)
        return 0
    for row in rows:
        if not row["alive"]:
            print("%s pid=%s dead" % (row["pile"], row["pid"]))
        elif not row.get("responsive"):
            print("%s pid=%s alive unresponsive" % (row["pile"], row["pid"]))
        else:
            extra = " detached" if row["detached"] else ""
            print("%s pid=%s alive gen=%d %s%s" % (row["pile"], row["pid"], row["generation"], row["vector"], extra))
    return 0


def cmd_state(cluster: Cluster, args) -> int:
    if args.pile is not None:
        _print(cluster.request(cluster.pile(args.pile), "GET", "/state"))
    else:
        out = {}
        for pile in cluster.live_piles():
            try:
                out[pile] = cluster.request(pile, "GET", "/state", timeout=2.0)
            except SupervisorError as e:
                out[pile] = {"error": str(e)}
        _print(out)
    return 0


def read_events(cluster: Cluster, pile: str) -> typing.List[dict]:
    out = []
    try:
        with open(cluster.path("logs", "%s.events.jsonl" % pile)) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        out.append(json.loads(line))
                    except ValueError:
                        pass
    except OSError:
        pass
    return out


def cmd_events(cluster: Cluster, args) -> int:
    piles = [cluster.pile(args.pile)] if args.pile is not None else cluster.piles()
    merged = []
    for pile in piles:
        for event in read_events(cluster, pile):
            if args.after_ts is not None and event["ts"] <= args.after_ts:
                continue
            if args.since is not None and event["seq"] <= args.since:
                continue
            if args.kind and not event["kind"].startswith(args.kind):
                continue
            if args.grep and args.grep not in json.dumps(event):
                continue
            merged.append(event)
    merged.sort(key=lambda e: (e["ts"], e["pile"], e["seq"]))
    if args.limit is not None:
        merged = merged[:args.limit]
    for event in merged:
        sys.stdout.write(json.dumps(event) + "\n")
    return 0


def cmd_now(cluster: Cluster, args) -> int:
    print("%.6f" % time.time())
    return 0


def _pid_or_fail(cluster: Cluster, pile: str) -> int:
    pile = cluster.pile(pile)
    pid = cluster.pid(pile)
    if pid is None or not cluster.alive(pile):
        raise SupervisorError("%s is not running" % pile)
    return pid


def cmd_kill(cluster: Cluster, args) -> int:
    pid = _pid_or_fail(cluster, args.pile)
    _signal(pid, signal.SIGKILL)
    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline and pid_alive(pid):
        time.sleep(0.02)
    _scenario(cluster, "kill", args.pile)
    return 0


def _scenario(cluster: Cluster, name: str, pile: str, **detail) -> None:
    """Tell a live instance that a macro happened, so the event log shows it."""
    for target in [pile] + [p for p in cluster.live_piles() if p != pile]:
        if not cluster.alive(target):
            continue
        try:
            cluster.request(target, "POST", "/scenario", {"name": name, "target": pile, **detail}, timeout=1.0)
            return
        except SupervisorError:
            continue


def revive_pile(cluster: Cluster, pile: str) -> None:
    if cluster.alive(pile):
        pid = cluster.pid(pile)
        _signal(pid, signal.SIGCONT)
        return
    proc = cluster.spawn(pile)
    cluster.wait_responsive([pile], {pile: proc})
    _reapply_macros(cluster, pile)


def _reapply_macros(cluster: Cluster, pile: str) -> None:
    """A new process starts clean: put it back on its side of a partition and under a keeper split."""
    for macro in cluster._macros():
        if macro["kind"] == "partition":
            groups = _groups(macro["spec"], cluster.piles())
            for group in groups:
                if pile in group:
                    others = [p for g in groups if g is not group for p in g]
                    cluster.request(pile, "POST", "/link", {"block": others})
        elif macro["kind"] == "keeper_split" and macro["pile"] == pile:
            cluster.request(pile, "POST", "/control", {"all": True, "op": "unavailable", "on": True})


def cmd_revive(cluster: Cluster, args) -> int:
    pile = cluster.pile(args.pile)
    revive_pile(cluster, pile)
    _scenario(cluster, "revive", pile)
    return 0


def cmd_stop(cluster: Cluster, args) -> int:
    pid = _pid_or_fail(cluster, args.pile)
    _scenario(cluster, "stop", args.pile)
    _signal(pid, signal.SIGSTOP)
    return 0


def cmd_cont(cluster: Cluster, args) -> int:
    pid = _pid_or_fail(cluster, args.pile)
    _signal(pid, signal.SIGCONT)
    _scenario(cluster, "cont", args.pile)
    return 0


def _control_body(target_kind: str, target: str, body: dict) -> dict:
    out = dict(body)
    if target_kind == "pile":
        out["all"] = True
    else:
        out["label"] = target
    return out


def _resolve_target(cluster: Cluster, target: str) -> typing.Tuple[str, str]:
    """A pile name targets all its nodes. A node label targets one node. Returns (pile, kind)."""
    topo = cluster.topology
    if target in topo.pile_names:
        return target, "pile"
    node = topo.node_by_label(target)
    if node is None:
        raise SupervisorError("unknown target %r: use a pile name or a node label" % target)
    return node.pile_name, "node"


def cmd_fault(cluster: Cluster, args) -> int:
    pile, kind = _resolve_target(cluster, args.target)
    body: dict = {"op": args.op}
    if args.op in ("down", "unavailable", "silence"):
        body["on"] = not args.off
        if args.max is not None:
            body["max"] = args.max
        if args.op == "silence" and args.methods:
            body["methods"] = [m for m in args.methods.split(",") if m]
    elif args.op == "delay":
        body["seconds"] = 0.0 if args.off else (args.seconds if args.seconds is not None else 1.0)
        if args.methods:
            body["methods"] = [m for m in args.methods.split(",") if m]
    elif args.op in ("storage", "connectivity"):
        body["piles"] = [] if args.off or args.piles is None else args.piles.split(",")
    elif args.op == "self_check":
        body["value"] = None if args.off else args.value
    cluster.request(pile, "POST", "/control", _control_body(kind, args.target, body))
    return 0


def cmd_node(cluster: Cluster, args, on: bool) -> int:
    node = cluster.topology.node_by_label(args.label)
    if node is None:
        raise SupervisorError("unknown node label %r" % args.label)
    cluster.request(node.pile_name, "POST", "/control", {"label": args.label, "op": "down", "on": on})
    return 0


def _groups(spec: str, piles: typing.Sequence[str]) -> typing.List[typing.List[str]]:
    has_empty = "" in piles
    groups = []
    for raw in spec.split("|"):
        tokens = raw.split(",")
        if has_empty:
            if raw == "":
                groups.append([""])
                continue
            if any(not t.strip() for t in tokens):
                raise SupervisorError(
                    'ambiguous blank pile name in partition group %r; write the empty pile name as a group '
                    'by itself, e.g. "|pile1"' % raw)
        group = [t.strip() for t in tokens if t.strip()]
        if group:
            groups.append(group)
    if len(groups) < 2:
        raise SupervisorError('partition needs at least two groups separated by "|", e.g. "pile1|pile2"')
    seen = set()
    for g in groups:
        for p in g:
            if p not in piles:
                raise SupervisorError("unknown pile %r in partition" % p)
            if p in seen:
                raise SupervisorError("pile %r appears in two groups" % p)
            seen.add(p)
    return groups


def cmd_partition(cluster: Cluster, args) -> int:
    groups = _groups(args.spec, cluster.piles())
    progress = _MutationReport("partition", cluster.piles())
    # Record intent before sending links: a failed/lost reply must still be undoable by heal.
    progress.step("record", "supervisor", lambda: cluster.record_macro({"kind": "partition", "spec": args.spec}))
    failures = []
    for i, group in enumerate(groups):
        others = [p for j, g in enumerate(groups) if j != i for p in g]
        for pile in group:
            if cluster.alive(pile):
                try:
                    progress.step("links", pile, lambda: cluster.request(pile, "POST", "/link", {"block": others}))
                except PartialMutationError as e:
                    failures.append(str(e))
            else:
                progress.data["results"].setdefault("links", {})[pile] = {"deferred_until_revive": True}
    if failures:
        raise PartialMutationError("could not partition every peer: %s" % "; ".join(failures), progress.data)
    progress.step("scenario", "supervisor", lambda: _scenario(cluster, "partition", groups[0][0], spec=args.spec))
    return 0


def cmd_heal(cluster: Cluster, args) -> int:
    macros = cluster._macros()
    progress = _MutationReport("heal", cluster.piles())
    errors = []
    for pile in cluster.live_piles():
        try:
            progress.step("links", pile, lambda: cluster.request(pile, "POST", "/link", {"set": []}))
        except PartialMutationError as e:
            errors.append(str(e))
    for macro in macros:
        if macro["kind"] == "keeper_split" and cluster.alive(macro["pile"]):
            for node in cluster.topology.pile(macro["pile"]).nodes:
                if node.label not in macro.get("already", []):
                    try:
                        progress.step("keeper_faults", node.label, lambda: cluster.request(
                            macro["pile"], "POST", "/control", {"label": node.label, "op": "unavailable", "on": False}))
                    except PartialMutationError as e:
                        errors.append(str(e))
    if errors:
        raise PartialMutationError("could not heal every fault: %s" % "; ".join(errors), progress.data)
    # Keep the records until every undo succeeds, so a failed heal can be retried.
    # Dead processes have no faults left; forgetting their macros keeps revival healed.
    progress.step("record", "supervisor", lambda: cluster._save_macros([]))
    progress.step("scenario", "supervisor", lambda: _scenario(cluster, "heal", cluster.piles()[0]))
    return 0


def cmd_keeper_split(cluster: Cluster, args) -> int:
    pile = cluster.pile(args.pile)
    # what was already unavailable before the macro is not the macro's to undo
    already = [n["label"] for n in cluster.request(pile, "GET", "/state")["nodes"]
               if n["faults"].get("unavailable")]
    cluster.request(pile, "POST", "/control", {"all": True, "op": "unavailable", "on": True})
    cluster.record_macro({"kind": "keeper_split", "pile": pile, "already": already})
    _scenario(cluster, "keeper_split", pile)
    return 0


def cmd_replica(cluster: Cluster, args) -> int:
    pile = cluster.pile(args.pile)
    if args.clear:
        cluster.request(pile, "POST", "/replica", {"clear": True})
        return 0
    states = {}
    for item in (args.states or "").split(","):
        if not item:
            continue
        if "=" not in item:
            raise SupervisorError("--states expects pile=STATE,..., got %r" % item)
        name, state = item.split("=", 1)
        states[name.strip()] = state.strip().upper()
    body = {"states": states, "detach": bool(args.detach)}
    if args.generation is not None:
        body["generation"] = args.generation
    cluster.request(pile, "POST", "/replica", body)
    return 0


def _parse_pairs(pairs: typing.Sequence[str]) -> dict:
    out = {}
    for item in pairs:
        if "=" not in item:
            raise SupervisorError("expected key=value, got %r" % item)
        key, raw = item.split("=", 1)
        try:
            out[key] = json.loads(raw)
        except ValueError:
            out[key] = raw
    return out


def cmd_settings(cluster: Cluster, args, path: str) -> int:
    body = _parse_pairs(args.pairs)
    for pile in cluster.live_piles():
        cluster.request(pile, "POST", path, body)
    return 0


def reset_cluster(cluster: Cluster) -> dict:
    piles = cluster.piles()
    progress = _MutationReport("reset", piles)
    prepared = []
    failure = None
    try:
        for pile in piles:
            if not cluster.alive(pile):
                progress.step("revive", pile, lambda: revive_pile(cluster, pile))
            else:
                progress.step("revive", pile, lambda: _signal(cluster.pid(pile), signal.SIGCONT))
        # Freeze every source of new decisions before sampling. Previously committed traffic can still
        # arrive, but its generation is bounded by the originating replica's persisted watermark.
        for pile in piles:
            prepared.append(pile)   # a lost reply may still mean that the barrier was entered
            progress.step("prepare", pile, lambda: cluster.request(pile, "POST", "/reset-prepare", {}))
        generations = []
        for pile in piles:
            generation = progress.step("sample", pile, lambda: _reset_generation(cluster, pile))
            generations.append(generation)
        new_generation = max(generations) + 1
        progress.data["generation"] = new_generation
        for pile in piles:
            progress.step("install", pile, lambda: cluster.request(pile, "POST", "/reset-local", {"generation": new_generation}))
    except PartialMutationError as e:
        failure = e
    finally:
        errors = []
        for pile in prepared:
            try:
                progress.step("resume", pile, lambda: cluster.request(pile, "POST", "/reset-cancel", {}))
            except PartialMutationError as e:
                errors.append(str(e))
        if errors:
            message = "could not resume piles after reset: %s" % "; ".join(errors)
            if failure is not None:
                sys.stderr.write("warning: %s\n" % message)
            else:
                failure = PartialMutationError(message, progress.data)
    if failure is not None:
        raise failure
    progress.step("record", "supervisor", lambda: cluster._save_macros([]))
    progress.step("scenario", "supervisor", lambda: _scenario(cluster, "reset", piles[0], generation=new_generation))
    return {"reset": True, "generation": new_generation}


def _reset_generation(cluster: Cluster, pile: str) -> int:
    state = cluster.request(pile, "GET", "/state")
    generation = state.get("max_generation", state["generation"])
    if isinstance(generation, bool) or not isinstance(generation, int) or generation < 0:
        raise SupervisorError("invalid generation returned by %s" % pile)
    return generation


def cmd_reset(cluster: Cluster, args) -> int:
    _print(reset_cluster(cluster))
    return 0


def cmd_endpoints(cluster: Cluster, args) -> int:
    _print(cluster.topology.endpoint_map())
    return 0


def cmd_manifest(cluster: Cluster, args) -> int:
    _print(cluster.manifest())
    return 0


def _body_fields(body: dict, allowed: typing.Set[str], required: typing.Set[str] = frozenset()) -> None:
    if set(body) - allowed:
        raise SupervisorError("unknown fields: %s" % ", ".join(sorted(set(body) - allowed)))
    if required - set(body):
        raise SupervisorError("missing fields: %s" % ", ".join(sorted(required - set(body))))


def _broadcast(cluster: Cluster, path: str, body: dict) -> dict:
    results, errors = {}, {}
    for pile in cluster.piles():
        if not cluster.alive(pile):
            errors[pile] = "instance is not running; change was not applied"
            continue
        try:
            results[pile] = cluster.request(pile, "POST", path, body)
        except SupervisorError as e:
            errors[pile] = str(e)
    return {"ok": not errors, "instances": results, "errors": errors}


def _http_action(cluster: Cluster, action: str, body: dict) -> dict:
    """Finite command allowlist; no supplied URL, filesystem path or shell command is executed."""
    if action == "process":
        _body_fields(body, {"pile", "action"}, {"pile", "action"})
        handlers = {"kill": cmd_kill, "revive": cmd_revive, "stop": cmd_stop, "cont": cmd_cont}
        if body["action"] not in handlers:
            raise SupervisorError("process action must be kill, revive, stop or cont")
        handlers[body["action"]](cluster, argparse.Namespace(pile=cluster.pile(body["pile"])))
    elif action in ("fault", "failpoints", "response-override", "rpc-override"):
        fields = {"fault": {"target", "op", "on", "max", "seconds", "methods", "piles", "value"},
                  "failpoints": {"target", "method", "stage", "action", "once"},
                  "response-override": {"target", "method", "result"},
                  "rpc-override": {"target", "method", "response", "raw_result", "once"}}
        _body_fields(body, fields[action], {"target"})
        pile, kind = _resolve_target(cluster, body["target"])
        args = {k: v for k, v in body.items() if k != "target"}
        path = "/control" if action == "fault" else "/" + action
        return cluster.request(pile, "POST", path, _control_body(kind, body["target"], args))
    elif action == "replica":
        _body_fields(body, {"pile", "clear", "generation", "states", "detach"}, {"pile"})
        return cluster.request(body["pile"], "POST", "/replica", {k: v for k, v in body.items() if k != "pile"})
    elif action == "partition":
        _body_fields(body, {"spec"}, {"spec"})
        if not isinstance(body["spec"], str):
            raise SupervisorError("partition spec must be a string")
        cmd_partition(cluster, argparse.Namespace(spec=body["spec"]))
    elif action in ("heal", "reset"):
        _body_fields(body, set())
        if action == "reset":
            return reset_cluster(cluster)
        cmd_heal(cluster, argparse.Namespace())
    elif action in ("timings", "policies"):
        _body_fields(body, {"values"}, {"values"})
        if not isinstance(body["values"], dict):
            raise SupervisorError("values must be an object")
        return _broadcast(cluster, "/" + action, body["values"])
    elif action == "followups":
        _body_fields(body, {"hold", "release", "error_reason"})
        return _broadcast(cluster, "/followups", body)
    elif action == "topology":
        _body_fields(body, {"pile", "configured_piles", "bridged", "self_management"})
        pile = body.get("pile")
        if pile is None:
            live = cluster.live_piles()
            if not live:
                raise SupervisorError("no live instance to configure topology")
            pile = live[0]
        return cluster.request(pile, "POST", "/topology", {k: v for k, v in body.items() if k != "pile"})
    else:
        raise SupervisorError("unknown supervisor action")
    return {"ok": True}


def _http_events(cluster: Cluster, cursors: dict, limit: int, kind: typing.Optional[str]) -> dict:
    if set(cursors) - set(cluster.piles()):
        raise SupervisorError("event cursor names an unknown pile")
    if any(isinstance(v, bool) or not isinstance(v, int) or v < 0 for v in cursors.values()):
        raise SupervisorError("event cursors must be nonnegative integers")
    if not 1 <= limit <= 10000:
        raise SupervisorError("event limit must be between 1 and 10000")
    streams, reset = {}, []
    next_cursors = {p: cursors.get(p, 0) for p in cluster.piles()}
    for pile in cluster.piles():
        events = read_events(cluster, pile)
        last = max((e["seq"] for e in events), default=0)
        if next_cursors[pile] > last:
            next_cursors[pile] = 0
            reset.append(pile)
        streams[pile] = iter(sorted((e for e in events if e["seq"] > next_cursors[pile] and (
            not kind or e["kind"].startswith(kind))), key=lambda e: e["seq"]))
    heap = []

    def advance(pile):
        event = next(streams[pile], None)
        if event is not None:
            heapq.heappush(heap, (event["ts"], pile, event["seq"], event))

    for pile in streams:
        advance(pile)
    selected = []
    while heap and len(selected) < limit:
        _, pile, _, event = heapq.heappop(heap)
        selected.append(event)
        advance(pile)
    for event in selected:
        next_cursors[event["pile"]] = max(next_cursors[event["pile"]], event["seq"])
    return {"events": selected, "cursors": next_cursors, "has_more": bool(heap), "reset_piles": reset}


def build_http_app(cluster: Cluster, supervisor_id: typing.Optional[str] = None) -> web.Application:
    """Cluster-level Lab API. Blocking process/control operations run outside the HTTP event loop."""
    identity = supervisor_id or uuid.uuid4().hex
    mutation_lock = asyncio.Lock()

    @web.middleware
    async def errors(request, handler):
        try:
            # A local browser cannot turn a hostile web page into a process-control client.
            origin = request.headers.get("Origin")
            if origin and origin != "%s://%s" % (request.scheme, request.host):
                return web.json_response({"error": "cross-origin requests are forbidden"}, status=403)
            host = request.host.rsplit(":", 1)[0].strip("[]")
            bound = request.transport.get_extra_info("sockname")[0]
            if host != "localhost":
                try:
                    address = ipaddress.ip_address(host)
                except ValueError:
                    return web.json_response({"error": "Host must name the listening address"}, status=403)
                if not address.is_loopback and host != bound:
                    return web.json_response({"error": "Host must name the listening address"}, status=403)
            return await handler(request)
        except PartialMutationError as e:
            return web.json_response(dict(e.report, error=str(e)), status=207)
        except (SupervisorError, TopologyError, ValueError, TypeError, KeyError) as e:
            return web.json_response({"error": str(e)}, status=400)

    app = web.Application(middlewares=[errors], client_max_size=1024 * 1024)

    async def manifest(request):
        return web.json_response(cluster.manifest(identity))

    async def state(request):
        out = await asyncio.to_thread(cluster.aggregate_state)
        out["supervisor_id"] = identity
        return web.json_response(out)

    async def events(request):
        cursors = json.loads(request.query.get("cursors", "{}"))
        if not isinstance(cursors, dict):
            raise SupervisorError("cursors must be a JSON object")
        out = await asyncio.to_thread(_http_events, cluster, cursors,
                                      int(request.query.get("limit", "1000")), request.query.get("kind"))
        out.update({"schema_version": 1, "cluster_id": cluster.manifest()["cluster_id"],
                    "supervisor_id": identity})
        return web.json_response(out)

    async def mutate(request):
        body = await request.json()
        if not isinstance(body, dict):
            raise SupervisorError("body must be a JSON object")
        async with mutation_lock:
            # Cancellation must not release serialization while a worker is still changing the cluster.
            task = asyncio.create_task(asyncio.to_thread(_http_action, cluster, request.match_info["action"], body))
            cancelled = False
            while not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    cancelled = True
            out = task.result()
            if cancelled:
                raise asyncio.CancelledError
        return web.json_response(out, status=200 if out.get("ok", True) else 207)

    app.router.add_get("/api/v1/manifest", manifest)
    app.router.add_get("/api/v1/state", state)
    app.router.add_get("/api/v1/events", events)
    actions = "process|fault|failpoints|response-override|rpc-override|replica|partition|heal|reset|timings|policies|followups|topology"
    app.router.add_post("/api/v1/{action:%s}" % actions, mutate)
    return app


def cmd_serve(cluster: Cluster, args) -> int:
    if args.host != "localhost":
        try:
            local = ipaddress.ip_address(args.host).is_loopback
        except ValueError:
            local = False
        if not local and not args.allow_remote:
            raise SupervisorError("supervisor control has no authentication; use a loopback host or --allow-remote")
    cluster.topology   # validate before starting the HTTP server
    web.run_app(build_http_app(cluster), host=args.host, port=args.port, access_log=None)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ydbd_mock cluster", description="Start, stop and control mock instances.")
    parser.add_argument("--run-dir", default=None, help="run directory (default $YDBD_MOCK_RUN_DIR)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    up = sub.add_parser("up", help="start one instance per pile")
    up.add_argument("--topology", default=None, help="use this topology file instead of the flags below")
    up.add_argument("--piles", default="pile1,pile2",
                    help="comma-separated nonempty pile names; use --topology for an explicit empty name")
    up.add_argument("--nodes-per-pile", type=int, default=3)
    up.add_argument("--cluster-name", default="mock-bridge")
    up.add_argument("--control-base-port", type=int, default=22000)
    up.add_argument("--grpc-base-port", type=int, default=22100)
    up.add_argument("--fresh", action="store_true", help="wipe persisted replicas first")
    up.add_argument("--log-level", default="INFO")
    up.add_argument("--set", action="append", help="timings.NAME=VALUE or policies.NAME=VALUE")
    up.set_defaults(fn=cmd_up)

    sub.add_parser("down", help="stop every instance").set_defaults(fn=cmd_down)
    status = sub.add_parser("status", help="one line per pile")
    status.add_argument("--json", action="store_true")
    status.set_defaults(fn=cmd_status)
    state = sub.add_parser("state", help="full /state of one pile or all")
    state.add_argument("pile", nargs="?")
    state.set_defaults(fn=cmd_state)
    events = sub.add_parser("events", help="merged event log, JSON lines")
    events.add_argument("--pile")
    events.add_argument("--after-ts", type=float, default=None)
    events.add_argument("--since", type=int, default=None, help="per-instance seq")
    events.add_argument("--kind", help="event kind prefix, e.g. commit or rpc.start")
    events.add_argument("--grep", help="substring of the JSON text")
    events.add_argument("--limit", type=int, default=None)
    events.set_defaults(fn=cmd_events)
    sub.add_parser("now", help="print the current epoch time").set_defaults(fn=cmd_now)

    for name, fn in (("kill", cmd_kill), ("revive", cmd_revive), ("stop", cmd_stop), ("cont", cmd_cont)):
        p = sub.add_parser(name, help="%s a pile's process" % name)
        p.add_argument("pile")
        p.set_defaults(fn=fn)

    fault = sub.add_parser("fault", help="per-node fault on a pile (all nodes) or one node label")
    fault.add_argument("target")
    fault.add_argument("--op", required=True,
                       choices=["down", "unavailable", "silence", "delay", "storage", "connectivity",
                                "self_check", "clear"])
    fault.add_argument("--off", action="store_true")
    fault.add_argument("--on", action="store_true")
    fault.add_argument("--seconds", type=float)
    fault.add_argument("--max", type=float)
    fault.add_argument("--methods")
    fault.add_argument("--piles")
    fault.add_argument("--value")
    fault.set_defaults(fn=cmd_fault)

    nd = sub.add_parser("node-down", help="close one node's listener")
    nd.add_argument("label")
    nd.set_defaults(fn=lambda c, a: cmd_node(c, a, True))
    nu = sub.add_parser("node-up", help="reopen one node's listener")
    nu.add_argument("label")
    nu.set_defaults(fn=lambda c, a: cmd_node(c, a, False))

    part = sub.add_parser("partition", help='cut peer links, e.g. "pile1|pile2"')
    part.add_argument("spec")
    part.set_defaults(fn=cmd_partition)
    sub.add_parser("heal", help="clear link blocks and macro faults").set_defaults(fn=cmd_heal)
    split = sub.add_parser("keeper-split", help="the keeper loses a pile, the cluster is fine")
    split.add_argument("pile")
    split.set_defaults(fn=cmd_keeper_split)

    replica = sub.add_parser("replica", help="override or detach a pile's replica")
    replica.add_argument("pile")
    replica.add_argument("--generation", type=int)
    replica.add_argument("--states", help="pile1=PRIMARY,pile2=DISCONNECTED")
    replica.add_argument("--detach", action="store_true")
    replica.add_argument("--clear", action="store_true")
    replica.set_defaults(fn=cmd_replica)

    for name, path in (("timings", "/timings"), ("policies", "/policies")):
        p = sub.add_parser(name, help="change %s on every live instance" % name)
        p.add_argument("pairs", nargs="+", metavar="key=value")
        p.set_defaults(fn=(lambda c, a, path=path: cmd_settings(c, a, path)))

    sub.add_parser("reset", help="revive everything and install a fresh snapshot at max generation + 1"
                   ).set_defaults(fn=cmd_reset)
    sub.add_parser("endpoints", help="print the label -> endpoint map").set_defaults(fn=cmd_endpoints)
    sub.add_parser("manifest", help="print versioned logical identities and dial endpoints").set_defaults(fn=cmd_manifest)
    serve = sub.add_parser("serve", help="serve the cluster Lab HTTP API in the foreground")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=21999)
    serve.add_argument("--allow-remote", action="store_true")
    serve.set_defaults(fn=cmd_serve)
    return parser


def main(argv: typing.Sequence[str]) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv))
    cluster = Cluster(args.run_dir)
    try:
        return args.fn(cluster, args)
    except (SupervisorError, TopologyError) as e:
        sys.stderr.write("ydbd_mock cluster: %s\n" % e)
        return 1


# used by tests and the e2e harness
def model_vector(state: dict) -> typing.List[str]:
    return [p["public"] for p in state["piles"]]
