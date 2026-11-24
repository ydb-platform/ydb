import asyncio
import os
from contextlib import AsyncExitStack, ExitStack, asynccontextmanager

import grpc
import pytest
import yatest.common

from eds_balancer import Balancer, FLOOR, GENERATION
from ydb.public.api.grpc import ydb_discovery_v1_pb2_grpc, ydb_scheme_v1_pb2_grpc, ydb_topic_v1_pb2_grpc
from ydb.public.api.grpc.draft.xds import eds_pb2_grpc
from ydb.public.api.grpc.draft.xds.wire import discovery_pb2 as xds
from ydb.public.api.protos import ydb_discovery_pb2, ydb_scheme_pb2, ydb_topic_pb2
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.oss.ydb_sdk_import import ydb


DATABASE = "/Root/tenant"
METADATA = (("x-ydb-database", DATABASE),)
TYPE_URL = "type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment"
MARKERS = ("eds-replica-a", "eds-replica-b")
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


async def endpoints(channel):
    unary = ydb_discovery_v1_pb2_grpc.DiscoveryServiceStub(channel)
    response = await unary.ListEndpoints(ydb_discovery_pb2.ListEndpointsRequest(database=DATABASE), metadata=METADATA, timeout=5)
    result = ydb_discovery_pb2.ListEndpointsResult()
    assert response.operation.status == StatusIds.SUCCESS and response.operation.result.Unpack(result), response
    return {(ep.address, ep.port) for ep in result.endpoints}


async def subscribe(channel, expected):
    stream = eds_pb2_grpc.EndpointDiscoveryServiceStub(channel).StreamEndpoints(metadata=METADATA, timeout=20)
    request = xds.DiscoveryRequest(node=xds.Node(id="test"), type_url=TYPE_URL, resource_names=[DATABASE])
    await stream.write(request)
    snapshot = await stream.read()
    assert snapshot.type_url == TYPE_URL and snapshot.version_info and snapshot.nonce, snapshot
    assert len(snapshot.resources) == 1 and snapshot.resources[0].type_url == TYPE_URL, snapshot
    assignment = xds.ClusterLoadAssignment.FromString(snapshot.resources[0].value)
    assert assignment.cluster_name == DATABASE, assignment
    actual = {(ep.endpoint.address.socket_address.address, ep.endpoint.address.socket_address.port_value)
              for locality in assignment.endpoints for ep in locality.lb_endpoints}
    assert actual == expected
    request.version_info, request.response_nonce = snapshot.version_info, snapshot.nonce
    await stream.write(request)
    return stream


async def check_stream_isolation(replicas):
    async with route_for(replicas) as (route, address, backends), grpc.aio.insecure_channel(address) as bootstrap:
        expected = [await endpoints(channel) for channel in backends]
        assert all(expected) and expected[0].isdisjoint(expected[1])
        assert await source_marker(backends[0]) == {MARKERS[0]}
        control = ydb_topic_v1_pb2_grpc.TopicServiceStub(bootstrap).StreamWrite(metadata=METADATA, timeout=25)
        writes = ydb_topic_pb2.StreamWriteMessage
        try:
            await control.write(writes.FromClient(init_request=writes.InitRequest(path=DATABASE + "/control", partition_id=0)))
            response = await control.read()
            assert response.status == StatusIds.SUCCESS and response.HasField("init_response"), response
            watch = await subscribe(bootstrap, expected[0])
            source, peer, _ = await asyncio.wait_for(route.subscriptions.get(), timeout=5)
            assert source == 0 and route.topic_peers == [peer]
            route.switch(1)
            assert await watch.code() == grpc.StatusCode.UNAVAILABLE
            assert (await watch.trailing_metadata())[GENERATION] == "2"
            replacement = await subscribe(bootstrap, expected[1])
            target, next_peer, _ = await asyncio.wait_for(route.subscriptions.get(), timeout=5)
            assert target == 1 and next_peer == peer
            replacement.cancel()

            write = writes.WriteRequest(codec=ydb_topic_pb2.CODEC_RAW)
            write.messages.add(seq_no=1, data=b"after-switch", uncompressed_size=12)
            await control.write(writes.FromClient(write_request=write))
            response = await control.read()
            assert response.status == StatusIds.SUCCESS, response
            assert [ack.seq_no for ack in response.write_response.acks] == [1], response
            assert await source_marker(backends[0]) == {MARKERS[0]}
        finally:
            control.cancel()


def test_eds_cut_leaves_bootstrap_connection_and_source_sessions_open(replicas):
    asyncio.run(check_stream_isolation(replicas))


@asynccontextmanager
async def sdk_process(address, probe_binary, workload="list-on-demand"):
    process = await asyncio.create_subprocess_exec(
        probe_binary, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
        env={**os.environ, "YDB_ENDPOINT": address, "YDB_DATABASE": DATABASE,
             "YDB_EDS_DISCOVERY": "1", "YDB_PROBE": workload, "YDB_PROBE_TOPIC": workload,
             "YDB_PROBE_ON_DEMAND": "1"},
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
        for target, announce in ((1, True), (0, False)):
            route.switch(target, announce_generation=announce)
            # The SDK must reopen discovery without a fresh application request.
            backend, next_peer, headers = await asyncio.wait_for(route.subscriptions.get(), timeout=8)
            assert backend == target and next_peer == peer
            if announce:
                assert headers.get(FLOOR) == str(route.generation)
            else:
                # A fixed UNAVAILABLE trailer carries no new floor. The worker
                # must verify the current route before dispatching this retry.
                assert headers.get(FLOOR) == str(route.generation - 1)
                assert headers.get("x-ydb-require-current-route") == "1"
            assert await list_with_sdk(sdk) == {MARKERS[target]}
            assert await source_marker(backends[1 - target]) == {MARKERS[1 - target]}
        assert not route.unary_requests, route.unary_requests


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
                    if index == 0:
                        controls.append(await stack.enter_async_context(direct.topic_client.writer(path, producer_id="seed")))
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
                # Write a unique message to the source after each discovery assignment.
                payload = ("read-%d" % target).encode()
                await controls[0].write_with_ack(payload)
                await expect_output(sdk, b"OK " + payload)
            elif workload == "topic-write":
                payload = ("write-%d" % target).encode()
                sdk.stdin.write(payload + b"\n")
                await sdk.stdin.drain()
                await expect_output(sdk, b"OK")
                message = await asyncio.wait_for(controls[0].receive_message(), timeout=12)
                assert message.data == payload
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


@pytest.mark.parametrize("workload", ["bulk-upsert", "topic-read", "topic-write"])
def test_bulk_moves_and_established_topics_drain(replicas, workload):
    asyncio.run(check_workload(replicas, yatest.common.binary_path(PROBE), workload))
