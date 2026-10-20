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
import json
import os
import signal
import subprocess
import sys
import time
import typing
import urllib.error
import urllib.request

from ydb.apps.bridge_skipper.ydbd_mock.lib.topology import Topology, TopologyError

DEFAULT_WORK = os.path.join(os.path.expanduser("~"), "ydbd_mock_work")


class SupervisorError(Exception):
    pass


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
        return ("--topology %s --pile %s " % (self.path("topology.yaml"), pile)) in argv

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
            "--pile", pile,
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
        piles = [p for p in args.piles.split(",") if p]
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
        json.dump({"argv_prefix": argv_prefix(), "log_level": args.log_level}, f)
    cluster._save_macros([])
    cluster._topo = None
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
    if args.pile:
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
    piles = [cluster.pile(args.pile)] if args.pile else cluster.piles()
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
        body["piles"] = [] if args.off else [p for p in (args.piles or "").split(",") if p]
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
    groups = [[p.strip() for p in g.split(",") if p.strip()] for g in spec.split("|")]
    groups = [g for g in groups if g]
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
    for i, group in enumerate(groups):
        others = [p for j, g in enumerate(groups) if j != i for p in g]
        for pile in group:
            if cluster.alive(pile):
                cluster.request(pile, "POST", "/link", {"block": others})
    cluster.record_macro({"kind": "partition", "spec": args.spec})
    _scenario(cluster, "partition", groups[0][0], spec=args.spec)
    return 0


def cmd_heal(cluster: Cluster, args) -> int:
    macros = cluster._macros()
    link_errors = []
    for pile in cluster.live_piles():
        try:
            cluster.request(pile, "POST", "/link", {"set": []})
        except SupervisorError as e:
            link_errors.append(str(e))
    for macro in macros:
        if macro["kind"] == "keeper_split" and cluster.alive(macro["pile"]):
            for node in cluster.topology.pile(macro["pile"]).nodes:
                if node.label not in macro.get("already", []):
                    cluster.request(macro["pile"], "POST", "/control",
                                    {"label": node.label, "op": "unavailable", "on": False})
    if link_errors:
        raise SupervisorError("could not heal peer links: %s" % "; ".join(link_errors))
    # Keep the records until every undo succeeds, so a failed heal can be retried.
    # Dead processes have no faults left; forgetting their macros keeps revival healed.
    cluster._save_macros([])
    _scenario(cluster, "heal", cluster.piles()[0])
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


def cmd_reset(cluster: Cluster, args) -> int:
    piles = cluster.piles()
    for pile in piles:
        if not cluster.alive(pile):
            revive_pile(cluster, pile)
        else:
            _signal(cluster.pid(pile), signal.SIGCONT)
    prepared = []
    try:
        # Freeze every source of new decisions before sampling. Previously committed traffic can still
        # arrive, but its generation is bounded by the originating replica's persisted watermark.
        for pile in piles:
            prepared.append(pile)   # a lost reply may still mean that the barrier was entered
            cluster.request(pile, "POST", "/reset-prepare", {})
        generations = []
        for pile in piles:
            state = cluster.request(pile, "GET", "/state")
            generations.append(state.get("max_generation", state["generation"]))
        new_generation = max(generations) + 1
        for pile in piles:
            cluster.request(pile, "POST", "/reset-local", {"generation": new_generation})
    finally:
        failed = sys.exc_info()[0] is not None
        errors = []
        for pile in prepared:
            try:
                cluster.request(pile, "POST", "/reset-cancel", {})
            except SupervisorError as e:
                errors.append(str(e))
        if errors:
            message = "could not resume piles after reset: %s" % "; ".join(errors)
            if failed:
                sys.stderr.write("warning: %s\n" % message)
            else:
                raise SupervisorError(message)
    cluster._save_macros([])
    _scenario(cluster, "reset", piles[0], generation=new_generation)
    _print({"reset": True, "generation": new_generation})
    return 0


def cmd_endpoints(cluster: Cluster, args) -> int:
    _print(cluster.topology.endpoint_map())
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ydbd_mock cluster", description="Start, stop and control mock instances.")
    parser.add_argument("--run-dir", default=None, help="run directory (default $YDBD_MOCK_RUN_DIR)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    up = sub.add_parser("up", help="start one instance per pile")
    up.add_argument("--topology", default=None, help="use this topology file instead of the flags below")
    up.add_argument("--piles", default="pile1,pile2")
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
