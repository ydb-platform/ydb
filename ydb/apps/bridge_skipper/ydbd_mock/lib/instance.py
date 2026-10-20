"""One mock process: one pile. gRPC servers (one per node), control and peer HTTP, the PileInstance.

`MockProcess` can run several times in one event loop (tests do that). `main()` is the
`ydbd_mock run` command.
"""
import argparse
import asyncio
import json
import logging
import signal
import sys
import typing

import grpc
from aiohttp import web

from ydb.apps.bridge_skipper.ydbd_mock.lib import control_api, services
from ydb.apps.bridge_skipper.ydbd_mock.lib.cluster import PileInstance
from ydb.apps.bridge_skipper.ydbd_mock.lib.events import EventLog
from ydb.apps.bridge_skipper.ydbd_mock.lib.faults import NodeRuntime
from ydb.apps.bridge_skipper.ydbd_mock.lib.peers import HttpTransport, Transport
from ydb.apps.bridge_skipper.ydbd_mock.lib.replica import ReplicaError
from ydb.apps.bridge_skipper.ydbd_mock.lib.topology import Topology, TopologyError

log = logging.getLogger("ydbd_mock")


def _server_credentials(tls: typing.Optional[dict]):
    """Best-effort TLS: `tls: {cert_file: ..., key_file: ...}` in the topology. None means plaintext."""
    if not tls:
        return None
    try:
        with open(tls["cert_file"], "rb") as f:
            cert = f.read()
        with open(tls["key_file"], "rb") as f:
            key = f.read()
    except (KeyError, OSError) as e:
        raise TopologyError("tls needs readable cert_file and key_file: %s" % e)
    return grpc.ssl_server_credentials([(key, cert)])


class NodeServer:
    """The gRPC listener of one node. `down` closes it, `up` opens it again on the same port."""

    def __init__(self, process: "MockProcess", index: int) -> None:
        self.process = process
        self.index = index
        self.node = process.inst.pile_info.nodes[index]
        self.server: typing.Optional["grpc.aio.Server"] = None

    @property
    def running(self) -> bool:
        return self.server is not None

    async def start(self) -> None:
        if self.server is not None:
            return
        p = self.process
        server = grpc.aio.server(
            options=[("grpc.so_reuseport", 0)],
            interceptors=[services.UnregisteredServiceInterceptor(p.events, self.node.label)],
        )
        credentials = _server_credentials(p.topo.tls)
        services.register(server, p.inst, p.runtimes[self.index], tls=credentials is not None)
        try:
            if credentials is not None:
                port = server.add_secure_port(self.node.endpoint, credentials)
            else:
                port = server.add_insecure_port(self.node.endpoint)
        except RuntimeError as e:
            raise OSError("cannot bind %s: %s" % (self.node.endpoint, e))
        if not port:
            raise OSError("cannot bind %s" % self.node.endpoint)
        await server.start()
        self.server = server

    async def stop(self, grace: typing.Optional[float] = 0.0) -> None:
        server, self.server = self.server, None
        if server is not None:
            await server.stop(grace)


class MockProcess:
    def __init__(
        self,
        topology: Topology,
        pile: typing.Union[str, int],
        state_dir: typing.Optional[str] = None,
        events_path: typing.Optional[str] = None,
        transport: typing.Optional[Transport] = None,
    ) -> None:
        self.topo = topology
        index = pile if isinstance(pile, int) else topology.pile_index(pile)
        self.events = EventLog(topology.pile_names[index], sink_path=events_path)
        self.transport = transport or HttpTransport(topology)
        self.inst = PileInstance(topology, index, self.transport, state_dir=state_dir, events=self.events)
        self.runtimes = [NodeRuntime(n, self.inst.node_faults[n.index_in_pile], self.events)
                         for n in self.inst.pile_info.nodes]
        self.nodes = [NodeServer(self, n.index_in_pile) for n in self.inst.pile_info.nodes]
        self._runner: typing.Optional[web.AppRunner] = None
        self._listener_lock = asyncio.Lock()
        self._listener_tasks: typing.Set["asyncio.Task"] = set()
        self._serving = False      # listeners are managed only between start() and stop()
        self.inst.on_listeners_changed = self._listeners_changed

    async def start(self) -> None:
        pile = self.inst.pile_info
        app = control_api.build_app(self)
        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        site = web.TCPSite(self._runner, pile.control_host, pile.control_port, reuse_address=True)
        try:
            await site.start()
        except OSError as e:
            await self._runner.cleanup()
            self._runner = None
            raise OSError("cannot bind control port %s: %s" % (pile.control_endpoint, e))
        try:
            await self.inst.start()      # loads the replica: it decides which listeners may open
            self._serving = True
            await self.reconcile_listeners(grace=0.0)
        except (OSError, ReplicaError):
            await self.stop()
            raise

    async def stop(self, grace: float = 1.0) -> None:
        self._serving = False
        for task in list(self._listener_tasks):
            task.cancel()
        await asyncio.gather(*self._listener_tasks, return_exceptions=True)
        async with self._listener_lock:
            await asyncio.gather(*(n.stop(grace) for n in self.nodes))
        await self.inst.stop()
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None
        await self.transport.close()
        self.events.close()

    # ----- listeners -----

    def _listener_wanted(self, index: int) -> bool:
        """A node listens unless it is faulted down, or its pile stopped serving (run.cpp)."""
        return not self.inst.node_faults[index].down and self.inst.serves()

    async def reconcile_listeners(self, grace: float = 1.0, drain_updates: bool = False) -> None:
        """Reconcile listeners, optionally waiting for active updates to send their replies first."""
        replies_by_node: typing.Dict[int, typing.Set["asyncio.Future"]] = {}
        while True:
            async with self._listener_lock:
                for node in self.nodes:
                    wanted = self._listener_wanted(node.index)
                    if wanted:
                        replies_by_node.pop(node.index, None)
                        if not node.running:
                            await node.start()
                    elif node.running:
                        if drain_updates:
                            # Snapshot only once per closing node: later requests cannot postpone closure.
                            replies = replies_by_node.setdefault(
                                node.index, set(self.runtimes[node.index].pending_update_replies))
                            pending = {reply for reply in replies if not reply.done()}
                            if pending:
                                break
                        await node.stop(grace)
                else:
                    return
            # Do not hold the listener lock while waiting: node-down and process shutdown can force a
            # close. asyncio.wait also leaves these shared futures intact if this task is cancelled.
            await asyncio.wait(pending)

    def _listeners_changed(self) -> None:
        if not self._serving:
            return
        task = asyncio.get_running_loop().create_task(self._reconcile_quietly())
        self._listener_tasks.add(task)
        task.add_done_callback(self._listener_tasks.discard)

    async def _reconcile_quietly(self) -> None:
        # Like run.cpp, wait for the disconnect request's response, even when peer delivery takes longer
        # than the ordinary grace period. Requests held by silence/delay faults have not started updates.
        try:
            await self.reconcile_listeners(grace=1.0, drain_updates=True)
        except OSError as e:
            self.events.append("listener.error", error=str(e))

    async def set_node_down(self, index: int, on: bool) -> None:
        self.inst.node_faults[index].down = on
        await self.reconcile_listeners(grace=0.0)

    def ready_info(self) -> dict:
        pile = self.inst.pile_info
        return {
            "ready": True,
            "pile": pile.name,
            "control": pile.control_endpoint,
            "nodes": [{"id": n.node_id, "label": n.label, "endpoint": n.endpoint} for n in pile.nodes],
        }


def apply_overrides(topology: Topology, assignments: typing.Sequence[str]) -> None:
    """`--set timings.promote_delay=0.5` or `--set policies.auto_blame_dead_piles=false`."""
    for item in assignments:
        if "=" not in item or "." not in item.split("=", 1)[0]:
            raise TopologyError("--set expects timings.NAME=VALUE or policies.NAME=VALUE, got %r" % item)
        key, raw = item.split("=", 1)
        section, name = key.split(".", 1)
        try:
            value = json.loads(raw)
        except ValueError:
            value = raw
        if section == "timings":
            topology.timings.update({name: value})
        elif section == "policies":
            topology.policies.update({name: value})
        else:
            raise TopologyError("unknown section %r in --set, use timings or policies" % section)


async def _run(args) -> int:
    topology = Topology.load(args.topology)
    apply_overrides(topology, args.set or [])
    state_dir = args.state_dir
    process = MockProcess(topology, args.pile, state_dir=state_dir, events_path=args.log_events)
    for warning in topology.warnings:
        log.warning(warning)
    try:
        await process.start()
    except (OSError, ReplicaError) as e:
        sys.stderr.write("ydbd_mock: %s\n" % e)
        return 1
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    sys.stdout.write(json.dumps(process.ready_info()) + "\n")
    sys.stdout.flush()
    await stop.wait()
    await process.stop(grace=1.0)
    return 0


def main(argv: typing.Sequence[str]) -> int:
    parser = argparse.ArgumentParser(prog="ydbd_mock run", description="Run one pile instance.")
    parser.add_argument("--topology", required=True, help="topology YAML file")
    parser.add_argument("--pile", required=True, help="pile name this instance serves")
    parser.add_argument("--state-dir", default=None, help="directory for replica.json (default: in memory)")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--log-events", default=None, help="also write the event log as JSON lines")
    parser.add_argument("--set", action="append", help="override timings.NAME=VALUE or policies.NAME=VALUE")
    args = parser.parse_args(list(argv))
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        return asyncio.run(_run(args))
    except (TopologyError, OSError, KeyError) as e:
        sys.stderr.write("ydbd_mock: %s\n" % e)
        return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
