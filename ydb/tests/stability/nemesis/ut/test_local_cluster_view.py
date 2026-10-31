from __future__ import annotations

from types import SimpleNamespace

from ydb.tests.stability.nemesis.internal.nemesis.local_cluster_view import (
    LocalClusterView,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import (
    TargetKind,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.cluster_inventory import (
    ClusterInventory,
)


def test_local_cluster_view_separates_identity_from_connection_address():
    cluster = SimpleNamespace(
        config=SimpleNamespace(
            static_erasure="block-4-2",
            yaml_config={
                "nameservice_config": {
                    "node": [
                        {
                            "node_id": 1,
                            "walle_location": {
                                "rack": "rack-a",
                                "data_center": "dc-a",
                            },
                        },
                        {
                            "node_id": 2,
                            "walle_location": {
                                "rack": "rack-b",
                                "data_center": "dc-a",
                            },
                        },
                    ]
                }
            },
        ),
        nodes={
            1: SimpleNamespace(
                host="localhost",
                grpc_port=31001,
                mon_port=31002,
                ic_port=31003,
            ),
            2: SimpleNamespace(
                host="localhost",
                grpc_port=31101,
                mon_port=31102,
                ic_port=31103,
            ),
        },
        slots={
            1: SimpleNamespace(
                node_id=9,
                host="localhost",
                grpc_port=32001,
                mon_port=32002,
                ic_port=32003,
            ),
        },
    )

    view = LocalClusterView.from_cluster(cluster)

    assert [node.logical_host for node in view.nodes.values()] == [
        "node-1",
        "node-2",
    ]
    assert {node.connect_host for node in view.nodes.values()} == {"localhost"}
    assert [node.grpc_port for node in view.nodes.values()] == [31001, 31101]
    assert [
        (
            slot.logical_host,
            slot.node_id,
            slot.connect_host,
            slot.grpc_port,
        )
        for slot in view.slots.values()
    ] == [("slot-1", 9, "localhost", 32001)]
    assert view.topology_document() == {
        "static_erasure": "block-4-2",
        "hosts": [
            {
                "name": "node-1",
                "location": {"rack": "rack-a", "data_center": "dc-a"},
            },
            {
                "name": "node-2",
                "location": {"rack": "rack-b", "data_center": "dc-a"},
            },
        ],
    }

    topology = SimpleNamespace(
        hosts={"node-1": object(), "node-2": object()},
    )
    inventory = ClusterInventory(topology, cluster_view=view)
    assert inventory.degraded_reason is None
    assert inventory.hosts == ["node-1", "node-2"]
    assert [
        (target.host, target.node_id, target.ic_port)
        for target in inventory.entities(TargetKind.NODE)
    ] == [
        ("node-1", 1, 31003),
        ("node-2", 2, 31103),
    ]
    assert [
        (target.host, target.slot_idx, target.node_id, target.ic_port)
        for target in inventory.entities(TargetKind.SLOT)
    ] == [
        ("slot-1", 1, 9, 32003),
    ]
