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
