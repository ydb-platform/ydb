from ydb.tests.library.harness.kikimr_client import kikimr_client_factory


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
