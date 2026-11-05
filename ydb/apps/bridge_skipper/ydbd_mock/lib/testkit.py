"""Test support: several MockProcess objects in one event loop, on free ports, with real gRPC and HTTP.

Used by the in-process tests (`test_services_grpc.py`, `test_control_api.py`). Not used by the
mock itself.
"""
import asyncio
import json
import os
import time
import typing

import aiohttp
import grpc

from ydb.apps.bridge_skipper.ydbd_mock.lib.instance import MockProcess, _finish_on_cancel
from ydb.apps.bridge_skipper.ydbd_mock.lib.topology import Policies, Timings, Topology
from ydb.public.api.grpc import ydb_config_v1_pb2_grpc
from ydb.public.api.grpc import ydb_discovery_v1_pb2_grpc
from ydb.public.api.grpc import ydb_monitoring_v1_pb2_grpc
from ydb.public.api.grpc.draft import ydb_bridge_v1_pb2_grpc


def tiny_timings(**over) -> Timings:
    values = dict(
        promote_delay=0.05, suspend_delay=0.05, connect_delay=0.05, sync_step_delay=0.02,
        followup_stagger=0.005, followup_retry=0.05, quorum_fail_delay=0.8,
        heartbeat_interval=0.03, heartbeat_timeout=0.4, peer_timeout=0.3, prepare_ttl=0.5)
    values.update(over)
    return Timings(**values)


async def wait_until(predicate, timeout: float = 5.0, step: float = 0.02, message: str = "condition") -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if asyncio.iscoroutine(result):
            result = await result
        if result:
            return
        await asyncio.sleep(step)
    raise AssertionError("timed out waiting for %s" % message)


class LiveCluster:
    def __init__(
        self,
        piles: int = 2,
        nodes: int = 3,
        timings: typing.Optional[Timings] = None,
        policies: typing.Optional[Policies] = None,
        state_root: typing.Optional[str] = None,
        tls: typing.Optional[dict] = None,
        root_certificates: typing.Optional[bytes] = None,
        initial: typing.Optional[dict] = None,
        pile_names: typing.Optional[typing.Sequence[str]] = None,
    ) -> None:
        names = list(pile_names) if pile_names is not None else ["pile%d" % (i + 1) for i in range(piles)]
        self.topo = Topology.with_free_ports(
            names, nodes, timings=timings or tiny_timings(), policies=policies or Policies(), tls=tls, initial=initial)
        self.root_certificates = root_certificates
        self.state_root = state_root
        self.procs: typing.Dict[str, MockProcess] = {}
        self._channels: typing.Dict[str, "grpc.aio.Channel"] = {}
        self._http: typing.Optional[aiohttp.ClientSession] = None

    def _new_process(self, pile: str) -> MockProcess:
        state_dir = os.path.join(self.state_root, pile) if self.state_root else None
        return MockProcess(self.topo, pile, state_dir=state_dir)

    async def start(self) -> "LiveCluster":
        for name in self.topo.pile_names:
            self.procs[name] = self._new_process(name)
        try:
            results = await _finish_on_cancel(asyncio.gather(
                *(p.start() for p in self.procs.values()), return_exceptions=True))
            for result in results:
                if isinstance(result, BaseException):
                    raise result
            self._http = aiohttp.ClientSession()
        except BaseException:
            await self.stop()
            raise
        return self

    async def stop(self) -> None:
        await _finish_on_cancel(self._stop())

    async def _stop(self) -> None:
        results = await asyncio.gather(
            *(channel.close(None) for channel in self._channels.values()), return_exceptions=True)
        errors = [r for r in results if isinstance(r, BaseException)]
        self._channels.clear()
        if self._http is not None:
            try:
                await self._http.close()
            except BaseException as e:
                errors.append(e)
            self._http = None
        results = await asyncio.gather(*(p.stop(0.0) for p in self.procs.values()), return_exceptions=True)
        errors.extend(r for r in results if isinstance(r, BaseException))
        if errors:
            raise errors[0]

    async def kill(self, pile: str) -> None:
        """What SIGKILL does to a real instance: every listener and the replica go away at once."""
        await self.procs[pile].stop(0.0)
        for label in [n.label for n in self.topo.pile(pile).nodes]:
            channel = self._channels.pop(label, None)
            if channel is not None:
                await channel.close(None)

    async def revive(self, pile: str) -> MockProcess:
        proc = self._new_process(pile)
        self.procs[pile] = proc
        await proc.start()
        return proc

    # ----- gRPC -----

    def channel(self, label: str) -> "grpc.aio.Channel":
        if label not in self._channels:
            node = self.topo.node_by_label(label)
            if self.topo.tls:
                credentials = grpc.ssl_channel_credentials(root_certificates=self.root_certificates)
                self._channels[label] = grpc.aio.secure_channel(
                    node.endpoint, credentials, options=[("grpc.ssl_target_name_override", "localhost")])
            else:
                self._channels[label] = grpc.aio.insecure_channel(node.endpoint)
        return self._channels[label]

    def bridge(self, label: str):
        return ydb_bridge_v1_pb2_grpc.BridgeServiceStub(self.channel(label))

    def discovery(self, label: str):
        return ydb_discovery_v1_pb2_grpc.DiscoveryServiceStub(self.channel(label))

    def config(self, label: str):
        return ydb_config_v1_pb2_grpc.ConfigServiceStub(self.channel(label))

    def monitoring(self, label: str):
        return ydb_monitoring_v1_pb2_grpc.MonitoringServiceStub(self.channel(label))

    def label(self, pile: str, n: int = 1) -> str:
        return self.topo.pile(pile).nodes[n - 1].label

    # ----- control HTTP -----

    async def http(self, pile: str, method: str, path: str, body: typing.Optional[dict] = None,
                   params: typing.Optional[dict] = None) -> typing.Tuple[int, typing.Any]:
        url = self.topo.control_url(pile) + path
        kwargs: dict = {}
        if body is not None:
            kwargs["data"] = json.dumps(body)
            kwargs["headers"] = {"Content-Type": "application/json"}
        if params:
            kwargs["params"] = params
        async with self._http.request(method, url, **kwargs) as response:
            text = await response.text()
            try:
                return response.status, json.loads(text)
            except ValueError:
                return response.status, text

    async def state(self, pile: str) -> dict:
        status, body = await self.http(pile, "GET", "/state")
        assert status == 200, body
        return body

    def inst(self, pile: str):
        return self.procs[pile].inst
