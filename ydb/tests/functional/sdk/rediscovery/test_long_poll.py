import asyncio
import os
from contextlib import AsyncExitStack, ExitStack, asynccontextmanager

import grpc
import pytest
import yatest.common

from run_long_poll_probe import established_sockets
from long_poll_balancer import Balancer, CONDITION, GENERATION, OPT_IN
from ydb.public.api.grpc import ydb_coordination_v1_pb2_grpc, ydb_discovery_v1_pb2_grpc, ydb_scheme_v1_pb2_grpc, ydb_topic_v1_pb2_grpc
from ydb.public.api.protos import ydb_coordination_pb2, ydb_discovery_pb2, ydb_scheme_pb2, ydb_topic_pb2
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.oss.ydb_sdk_import import ydb


DATABASE = "/Root/tenant"
METADATA = (("x-ydb-database", DATABASE),)
MARKERS = ("poll-replica-a", "poll-replica-b")
PROBE = "ydb/tests/functional/sdk/rediscovery/clients/cpp/rediscovery-probe"


@pytest.fixture(scope="module")
def replicas():
    with ExitStack() as stack:
        addresses = []
        for marker in MARKERS:
            cluster = KiKiMR(KikimrConfigGenerator(nodes=1), cluster_name=marker)
            stack.callback(cluster.stop)
            cluster.start()
            cluster.create_database(DATABASE, storage_pool_units_count={"hdd": 1})
            cluster.register_and_start_slots(DATABASE, count=1)
            cluster.wait_tenant_up(DATABASE)
            node = next(iter(cluster.slots.values()))
            address = "localhost:%d" % node.grpc_port
            with grpc.insecure_channel(address) as channel:
                scheme = ydb_scheme_v1_pb2_grpc.SchemeServiceStub(channel)
                result = scheme.MakeDirectory(
                    ydb_scheme_pb2.MakeDirectoryRequest(path=DATABASE + "/" + marker), metadata=METADATA, timeout=10
                )
                assert result.operation.status == StatusIds.SUCCESS, result
                coord = ydb_coordination_v1_pb2_grpc.CoordinationServiceStub(channel)
                result = coord.CreateNode(ydb_coordination_pb2.CreateNodeRequest(path=DATABASE + "/coordination"),
                                          metadata=METADATA, timeout=10)
                assert result.operation.status == StatusIds.SUCCESS, result
                topics = ydb_topic_v1_pb2_grpc.TopicServiceStub(channel)
                result = topics.CreateTopic(
                    ydb_topic_pb2.CreateTopicRequest(path=DATABASE + "/control"), metadata=METADATA, timeout=10
                )
                assert result.operation.status == StatusIds.SUCCESS, result
            addresses.append(address)
        yield addresses


@asynccontextmanager
async def route_for(replicas):
    async with AsyncExitStack() as stack:
        backends = [await stack.enter_async_context(grpc.aio.insecure_channel(addr)) for addr in replicas]
        route = Balancer(DATABASE, backends)
        address = await stack.enter_async_context(route.serve())
        yield route, address, backends


async def source_marker(channel):
    scheme = ydb_scheme_v1_pb2_grpc.SchemeServiceStub(channel)
    response = await scheme.ListDirectory(ydb_scheme_pb2.ListDirectoryRequest(path=DATABASE), metadata=METADATA, timeout=5)
    result = ydb_scheme_pb2.ListDirectoryResult()
    assert response.operation.status == StatusIds.SUCCESS and response.operation.result.Unpack(result), response
    return {child.name for child in result.children}.intersection(MARKERS)


async def check_protocol(replicas):
    async with route_for(replicas) as (route, address, _), grpc.aio.insecure_channel(address) as bootstrap:
        unary = ydb_discovery_v1_pb2_grpc.DiscoveryServiceStub(bootstrap)
        request = ydb_discovery_pb2.ListEndpointsRequest(database=DATABASE)
        legacy = unary.ListEndpoints(request, metadata=METADATA, timeout=3)
        assert (await legacy).operation.status == StatusIds.SUCCESS
        assert GENERATION not in dict(await legacy.trailing_metadata())
        headers = METADATA + ((OPT_IN, "1"),)
        first = unary.ListEndpoints(request, metadata=headers, timeout=3)
        source = await first
        assert (await first.trailing_metadata())[GENERATION] == "1"
        poll = unary.ListEndpoints(request, metadata=headers + ((CONDITION, "1"),), timeout=5)
        await asyncio.wait_for(route.subscriptions.get(), timeout=2)
        assert not poll.done()
        route.switch(1)
        other = await poll
        assert other.operation.status == StatusIds.SUCCESS
        assert other.operation.result != source.operation.result
        assert (await poll.trailing_metadata())[GENERATION] == "2"
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await unary.ListEndpoints(request, metadata=headers + ((CONDITION, "3"),), timeout=3)
        assert error.value.code() == grpc.StatusCode.UNAVAILABLE
        route.hold = 0.02
        renewal = unary.ListEndpoints(request, metadata=headers + ((CONDITION, "2"),), timeout=3)
        assert (await renewal).operation.status == StatusIds.SUCCESS
        assert (await renewal.trailing_metadata())[GENERATION] == "2"
        legacy = unary.ListEndpoints(request, metadata=METADATA, timeout=3)
        assert (await legacy).operation.result == other.operation.result
        assert GENERATION not in dict(await legacy.trailing_metadata())


def test_conditional_unary_discovery(replicas):
    asyncio.run(check_protocol(replicas))


@asynccontextmanager
async def sdk_process(address, probe_binary, workload="list-on-demand"):
    process = await asyncio.create_subprocess_exec(
        probe_binary, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
        env={**os.environ, "YDB_ENDPOINT": address, "YDB_DATABASE": DATABASE,
             "YDB_LONG_POLL_DISCOVERY": "1", "YDB_PROBE": workload, "YDB_PROBE_TOPIC": workload},
    )
    try:
        yield process
    finally:
        if process.returncode is None:
            process.kill()
        await process.wait()


async def list_with_sdk(process):
    process.stdin.write(b"list\n")
    await process.stdin.drain()
    line = (await asyncio.wait_for(process.stdout.readline(), timeout=10)).decode().strip().split()
    assert line and line[0] == "ENTRIES", line
    return set(line[1:]).intersection(MARKERS)


async def check_idle_driver(replicas, probe_binary):
    async with route_for(replicas) as (route, address, backends), sdk_process(address, probe_binary) as sdk:
        assert await list_with_sdk(sdk) == {MARKERS[0]}
        source, peer, _ = await asyncio.wait_for(route.subscriptions.get(), timeout=8)
        assert source == 0
        for target in (1, 0):
            route.switch(target)
            # The SDK must issue the next conditional poll without a fresh application request.
            backend, next_peer, headers = await asyncio.wait_for(route.subscriptions.get(), timeout=8)
            assert backend == target and next_peer == peer
            assert headers[CONDITION] == str(route.generation)
            assert await list_with_sdk(sdk) == {MARKERS[target]}
            assert await source_marker(backends[1 - target]) == {MARKERS[1 - target]}


def test_idle_cpp_driver_recovers_in_both_directions(replicas):
    asyncio.run(check_idle_driver(replicas, yatest.common.binary_path(PROBE)))


async def expect_output(process, expected):
    async def read_lines():
        while line := await process.stdout.readline():
            if line.strip() == expected:
                return
        raise AssertionError(f"C++ probe exited with status {await process.wait()}")
    await asyncio.wait_for(read_lines(), timeout=12)


async def check_workload(replicas, probe_binary, workload):
    async with AsyncExitStack() as stack:
        controls = []
        path = DATABASE + "/" + workload
        for index, address in enumerate(replicas):
            direct = await stack.enter_async_context(ydb.aio.Driver(
                endpoint="grpc://" + address, database=DATABASE, credentials=ydb.AnonymousCredentials()
            ))
            await direct.wait(timeout=10)
            if workload == "bulk-upsert":
                pool = await stack.enter_async_context(ydb.aio.QuerySessionPool(direct))
                await pool.execute_with_retries("CREATE TABLE probe (id Uint64 NOT NULL, PRIMARY KEY(id))")
                controls.append(pool)
            else:
                await direct.topic_client.create_topic(path, consumers=[ydb.TopicConsumer("probe")])
                if workload == "topic-read":
                    async with direct.topic_client.writer(path, producer_id="seed") as writer:
                        await writer.write_with_ack(MARKERS[index].encode())
                else:
                    controls.append(await stack.enter_async_context(direct.topic_client.reader(path, consumer="probe")))

        route, address, _ = await stack.enter_async_context(route_for(replicas))
        sdk = await stack.enter_async_context(sdk_process(address, probe_binary, workload))
        for target in (0, 1):
            if target:
                route.switch(target)
            selected, _, _ = await asyncio.wait_for(route.subscriptions.get(), timeout=8)
            assert selected == target
            if workload == "topic-read":
                await expect_output(sdk, ("OK " + MARKERS[target]).encode())
            elif workload == "topic-write":
                await expect_output(sdk, b"OK")
                message = await asyncio.wait_for(controls[target].receive_message(), timeout=12)
                assert message.data == b"probe"
            else:
                # Check independent database state. The probe can also print OK for a source write in flight.
                async def wait_for_row():
                    while True:
                        result = await controls[target].execute_with_retries("SELECT id FROM probe")
                        if [row.id for row in result[0].rows] == [1]:
                            return
                        await asyncio.sleep(0.05)
                await expect_output(sdk, b"OK")
                await asyncio.wait_for(wait_for_row(), timeout=12)
        old_port = int(replicas[0].rsplit(":", 1)[1])
        async with asyncio.timeout(5):
            while established_sockets(sdk.pid, {old_port}) != 0:
                await asyncio.sleep(0.025)


@pytest.mark.parametrize("workload", ["bulk-upsert", "topic-read", "topic-write"])
def test_cpp_data_client_moves_to_destination(replicas, workload):
    asyncio.run(check_workload(replicas, yatest.common.binary_path(PROBE), workload))


async def check_retained_session(replicas, probe, workload):
    port = int(replicas[0].rsplit(":", 1)[1])
    async with route_for(replicas) as (route, address, _), sdk_process(address, probe, workload) as sdk:
        await expect_output(sdk, b"READY")
        sdk.stdin.write(b"check\n")
        await sdk.stdin.drain()
        await expect_output(sdk, b"ACTIVE")
        source, peer, _ = await asyncio.wait_for(route.subscriptions.get(), timeout=5)
        assert source == 0 and established_sockets(sdk.pid, {port}) == 1
        route.switch(1)
        target, next_peer, _ = await asyncio.wait_for(route.subscriptions.get(), timeout=5)
        assert target == 1 and next_peer == peer
        # Keep the application idle and its original session alive while checking TCP.
        async with asyncio.timeout(5):
            while established_sockets(sdk.pid, {port}) != 0:
                await asyncio.sleep(0.025)
        sdk.stdin.write(b"check\n")
        await sdk.stdin.drain()
        await expect_output(sdk, b"EXPIRED")


@pytest.mark.parametrize("workload", ["query-session", "table-session", "coordination-session"])
def test_retained_session_releases_source_connection(replicas, workload):
    asyncio.run(check_retained_session(replicas, yatest.common.binary_path(PROBE), workload))
