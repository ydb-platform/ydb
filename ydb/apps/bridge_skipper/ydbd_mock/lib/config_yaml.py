"""The cluster config YAML that `admin cluster config fetch` prints.

Rendered from the topology and identical on every node. The CLI rewrites
`metadata.version` before printing, so `metadata` must be valid. The demo reads
`config.hosts[].host` and `config.hosts[].location.bridge_pile_name`, with hosts in
node-id order, so piles come out in pile-index order.
"""
import yaml

INTERCONNECT_PORT = 19001  # what real configs carry; nothing listens on it


def render(topology, version: int = 1, state=None) -> str:
    names = topology.pile_names if state is None or state.configured_piles is None else state.configured_piles
    hosts = []
    for node in topology.all_nodes():
        if node.pile_name not in names:
            continue
        hosts.append({
            "host": node.label,
            "host_config_id": 1,
            "node_id": node.node_id,
            "port": INTERCONNECT_PORT,
            "location": {
                "data_center": node.data_center,
                "bridge_pile_name": node.pile_name,
            },
        })
    doc = {
        "metadata": {"kind": "MainConfig", "cluster": topology.cluster_name, "version": version},
        "config": {
            "bridge_config": {"piles": [{"name": name} for name in names]},
            "hosts": hosts,
        },
    }
    return yaml.safe_dump(doc, sort_keys=False, default_flow_style=False)
