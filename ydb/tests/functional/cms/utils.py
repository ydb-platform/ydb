from hamcrest import assert_that, equal_to, greater_than, has_length, has_properties

from ydb.core.protos import cms_pb2
from ydb.tests.library.common import protobuf_cms
from ydb.tests.library.common.wait_for import wait_for_and_assert
from ydb.tests.library.clients.kikimr_client import kikimr_client_factory
from ydb.tests.library.clients.kikimr_keyvalue_client import keyvalue_client_factory
from ydb.tests.library.clients.kikimr_scheme_client import scheme_client_factory


def value_for(key, tablet_id):
    return "Value: <key = {key}, tablet_id = {tablet_id}>".format(
        key=key, tablet_id=tablet_id)


def create_client_from_alive_hosts(cluster, alive_nodeids):
    alive_node = cluster.nodes[1]
    for node_id, node in cluster.nodes.items():
        if node_id not in alive_nodeids:
            alive_node = node
            break
    assert alive_node not in alive_nodeids
    client = kikimr_client_factory(alive_node.host, alive_node.grpc_port, retry_count=100)

    return client


def create_kv_client_from_alive_hosts(cluster, alive_nodeids):
    alive_node = cluster.nodes[1]
    for node_id, node in cluster.nodes.items():
        if node_id not in alive_nodeids:
            alive_node = node
            break
    assert alive_node not in alive_nodeids
    client = keyvalue_client_factory(alive_node.host, alive_node.grpc_port, retry_count=100)

    return client


def create_scheme_client_from_alive_hosts(cluster, alive_nodeids):
    alive_node = cluster.nodes[1]
    for node_id, node in cluster.nodes.items():
        if node_id not in alive_nodeids:
            alive_node = node
            break
    assert alive_node not in alive_nodeids
    client = scheme_client_factory(alive_node.host, alive_node.grpc_port, retry_count=100)

    return client


def configure_cms_and_wait(cluster, mode):
    config = protobuf_cms.CmsConfigRequest()
    # Keep the production cooldown at two minutes; these tests exercise restart
    # availability rules, not the cooldown itself.
    config.protobuf.SetConfigRequest.Config.StateStorageRestartCooldown = 1000000
    response = cluster.client.send_request(config.protobuf, "CmsRequest")
    assert_that(response.SetConfigResponse.Status.Code, equal_to(cms_pb2.TStatus.OK), str(response))

    request = protobuf_cms.CmsPermissionRequest()
    request.protobuf.PermissionRequest.DryRun = True
    request.set_mode(mode)
    for node_id in cluster.nodes:
        request.add_action(str(node_id), cms_pb2.TAction.SHUTDOWN_HOST)

    # Probe the same checks as the real request, without reserving any nodes.
    wait_for_and_assert(
        lambda: cluster.client.send_request(request.protobuf, "CmsRequest").PermissionResponse,
        has_properties(Permissions=has_length(greater_than(0))),
        message="CMS did not become ready to grant a restart permission",
        timeout_seconds=120,
    )
