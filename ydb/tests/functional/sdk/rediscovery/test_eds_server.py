import asyncio

import grpc
import pytest

from ydb.public.api.grpc import ydb_discovery_v1_pb2_grpc as unary
from ydb.public.api.grpc.draft.xds import eds_pb2_grpc as eds
from ydb.public.api.grpc.draft.xds.wire import discovery_pb2 as wire
from ydb.public.api.protos import ydb_discovery_pb2 as discovery
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR


DATABASE = "/Root/tenant"
TOKEN = "root@builtin"
TYPE = "type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment"
AUTH = (("x-ydb-auth-ticket", TOKEN), ("x-ydb-database", DATABASE))


@pytest.fixture(scope="module")
def secured_cluster():
    config = KikimrConfigGenerator(nodes=1, enforce_user_token_requirement=True, default_clusteradmin=TOKEN)
    config.yaml_config.setdefault("auth_config", {})["staff_api_user_token"] = TOKEN
    config.yaml_config["feature_flags"]["check_database_access_permission"] = True
    security = config.yaml_config["domains_config"]["security_config"]
    security["default_access"] = ["+F:" + TOKEN]
    security["register_dynamic_node_allowed_sids"] = [TOKEN]
    cluster = KiKiMR(config, cluster_name="eds-security")
    try:
        cluster.start()
        cluster.create_database(DATABASE, storage_pool_units_count={"hdd": 1}, token=TOKEN)
        slot, = cluster.register_and_start_slots(DATABASE)
        cluster.wait_tenant_up(DATABASE, token=TOKEN)
        yield cluster, "localhost:%d" % slot.grpc_port
    finally:
        cluster.stop()


def subscribe(database=DATABASE):
    return wire.DiscoveryRequest(resource_names=[database], type_url=TYPE)


def assignment_ports(reply):
    assert reply.type_url == TYPE and reply.nonce and reply.version_info
    resource, = reply.resources
    assert resource.type_url == TYPE
    assignment = wire.ClusterLoadAssignment.FromString(resource.value)
    assert assignment.cluster_name == DATABASE
    return {lb.endpoint.address.socket_address.port_value for group in assignment.endpoints for lb in group.lb_endpoints}


async def check_security(address):
    async with grpc.aio.insecure_channel(address) as channel:
        stream = eds.EndpointDiscoveryServiceStub(channel).StreamEndpoints
        list_endpoints = unary.DiscoveryServiceStub(channel).ListEndpoints
        for metadata in ((), (("x-ydb-database", DATABASE), ("x-ydb-database", DATABASE))):
            watch = stream(metadata=metadata, timeout=10)
            status = await watch.code()
            assert status == grpc.StatusCode.INVALID_ARGUMENT, status

        for headers, code in [
            ((("x-ydb-database", DATABASE),), grpc.StatusCode.UNAUTHENTICATED),
            ((("x-ydb-database", DATABASE), ("x-ydb-auth-ticket", "eds-nobody@builtin")), grpc.StatusCode.PERMISSION_DENIED),
        ]:
            watch = stream(metadata=headers, timeout=10)
            await watch.write(subscribe())
            status = await watch.code()
            assert status == code, status
            try:
                result = await list_endpoints(discovery.ListEndpointsRequest(database=DATABASE), metadata=headers, timeout=10)
            except grpc.aio.AioRpcError as err:
                assert err.code() == grpc.StatusCode.UNAUTHENTICATED
            else:
                assert result.operation.status == StatusIds.UNAUTHORIZED

        watch = stream(metadata=AUTH, timeout=10)
        await watch.write(subscribe("/Root"))
        assert await watch.code() == grpc.StatusCode.INVALID_ARGUMENT

        watch = stream(metadata=AUTH, timeout=10)
        await watch.write(subscribe())
        assert assignment_ports(await watch.read())
        await watch.write(subscribe("/Root"))
        assert await watch.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_only_the_authenticated_database_can_be_watched(secured_cluster):
    _, address = secured_cluster
    asyncio.run(check_security(address))


async def check_membership(address, start_slot, stop_slot):
    async with grpc.aio.insecure_channel(address) as channel:
        service = eds.EndpointDiscoveryServiceStub(channel)
        watch = service.StreamEndpoints(metadata=AUTH, timeout=90)
        await watch.write(subscribe())
        first = await watch.read()
        expected = assignment_ports(first)
        assert len(expected) == 1

        extra = await asyncio.to_thread(start_slot)
        try:
            expected_more = expected | {extra.grpc_port}
            list_endpoints = unary.DiscoveryServiceStub(channel).ListEndpoints

            async def wait_for_board():
                while True:
                    reply = await list_endpoints(discovery.ListEndpointsRequest(database=DATABASE), metadata=AUTH, timeout=5)
                    result = discovery.ListEndpointsResult()
                    assert reply.operation.result.Unpack(result)
                    if {endpoint.port for endpoint in result.endpoints} == expected_more:
                        return
                    await asyncio.sleep(0.1)

            await asyncio.wait_for(wait_for_board(), timeout=30)

            stale = subscribe()
            stale.response_nonce, stale.version_info = "stale", first.version_info
            await watch.write(stale)
            update = asyncio.create_task(watch.read())
            assert not (await asyncio.wait([update], timeout=0.25))[0]

            nack = subscribe()
            nack.response_nonce = first.nonce
            nack.error_detail.code = 3
            nack.error_detail.message = "client rejected the first snapshot"
            await watch.write(nack)
            second = await asyncio.wait_for(update, timeout=15)
            assert assignment_ports(second) == expected_more
            assert second.nonce != first.nonce and second.version_info != first.version_info
            ack = subscribe()
            ack.version_info, ack.response_nonce = second.version_info, second.nonce
            await watch.write(ack)
        finally:
            await asyncio.to_thread(stop_slot, extra)

        third = await asyncio.wait_for(watch.read(), timeout=20)
        assert assignment_ports(third) == expected and third.nonce != second.nonce
        ack.version_info, ack.response_nonce = third.version_info, third.nonce
        await watch.write(ack)
        watch.cancel()
        assert await watch.code() == grpc.StatusCode.CANCELLED

        replacement = service.StreamEndpoints(metadata=AUTH, timeout=10)
        await replacement.write(subscribe())
        assert assignment_ports(await replacement.read()) == expected
        replacement.cancel()


def test_acknowledgement_backpressure_and_live_board_membership(secured_cluster):
    cluster, address = secured_cluster
    asyncio.run(check_membership(address, lambda: cluster.register_and_start_slots(DATABASE)[0],
                                 lambda slot: cluster.unregister_and_stop_slots([slot])))
