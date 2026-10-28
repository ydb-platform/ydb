import ydb

from common import BridgeKiKiMRTest
from ydb.public.api.protos.ydb_bridge_common_pb2 import PileState


class TestMixedStorageDatabases(BridgeKiKiMRTest):
    nodes_count = 24
    pile_names = ("r1", "r2", "r3")

    def configure_cluster(self):
        config = self.configurator.yaml_config
        config["bridge_config"]["enable_non_uniform_storage"] = True
        for node_id, host in enumerate(config["hosts"], 1):
            location = host["location"]
            location["data_center"] = location["bridge_pile_name"]
            location["rack"] = f"rack-{node_id}"
            location["unit"] = str(node_id)
        config["storage_pool_types"] = [
            {
                "kind": kind,
                "pool_config": {
                    "box_id": 1,
                    "kind": kind,
                    "erasure_species": "block-4-2" if bridged else "mirror-3-dc",
                    "bridge_mode": bridged,
                    "vdisk_kind": "Default",
                    "pdisk_filter": [{"property": [{"type": "ROT"}]}],
                },
            }
            for kind, bridged in (("bridged", True), ("ordinary", False))
        ]

    def setup_method(self):
        self.drivers = []
        self.sessions = {}
        super().setup_method()
        try:
            for kind in ("bridged", "ordinary"):
                database = f"/Root/{kind}"
                self.cluster.create_database(database, storage_pool_units_count={kind: 1})
                slots = []
                for pile in self.pile_names:
                    slots.extend(self.cluster.register_and_start_slots(database, bridge_pile_name=pile))
                self.cluster.wait_tenant_up(database)
                driver = ydb.Driver(ydb.DriverConfig(
                    endpoint=f"grpc://{slots[0].endpoint}", database=database, disable_discovery=True,
                ))
                self.drivers.append(driver)
                driver.wait(timeout=30)
                self.sessions[kind] = ydb.SessionPool(driver, size=1)
        except Exception:
            self.teardown_method()
            raise

    def teardown_method(self):
        try:
            for pool in self.sessions.values():
                pool.stop()
            for driver in self.drivers:
                self.stop_driver(driver)
        finally:
            super().teardown_method()

    def execute(self, kind, query, scheme=False):
        def operation(session):
            settings = ydb.BaseRequestSettings().with_timeout(30)
            if scheme:
                return session.execute_scheme(query, settings=settings)
            return session.transaction(ydb.SerializableReadWrite()).execute(query, commit_tx=True, settings=settings)
        return self.sessions[kind].retry_operation_sync(operation)

    def check_pools(self, count):
        pools = {pool.Name: pool for pool in self.cluster.client.read_storage_pools()}
        config = self.cluster.client.query_base_config().BaseConfig
        nodes = {node.NodeId: node for node in config.Node}
        for kind in ("bridged", "ordinary"):
            pool = pools[f"/Root/{kind}:{kind}"]
            assert pool.HasField("BridgeMode")
            assert pool.BridgeMode == (kind == "bridged")
            assert pool.NumGroups == count
            groups = [group for group in config.Group if group.StoragePoolId == pool.StoragePoolId]
            physical = [group for group in groups if group.VSlotId]
            if kind == "ordinary":
                assert len(groups) == len(physical) == count
                for group in groups:
                    assert group.ErasureSpecies == "mirror-3-dc"
                    assert len(group.VSlotId) == 9
                    assert len({nodes[slot.NodeId].Location.DataCenter for slot in group.VSlotId}) == 3
            else:
                assert len(groups) == 4 * count
                assert len(physical) == 3 * count
                for group in physical:
                    assert group.ErasureSpecies == "block-4-2"
                    assert len({nodes[slot.NodeId].Location.BridgePileName for slot in group.VSlotId}) == 1

    def test_create_grow_and_promote(self):
        for kind in self.sessions:
            self.execute(kind, "CREATE TABLE data (id Uint64, value Utf8, PRIMARY KEY(id));", scheme=True)
            self.execute(kind, 'UPSERT INTO data (id, value) VALUES (1u, "before-promotion");')
        self.check_pools(1)

        self.update_cluster_state(self.bridge_client, [PileState(pile_name="r2", state=PileState.PROMOTED)])
        self.wait_for_cluster_state(self.secondary_bridge_client, {
            "r1": PileState.SYNCHRONIZED, "r2": PileState.PRIMARY, "r3": PileState.SYNCHRONIZED,
        })
        for kind in self.sessions:
            self.execute(kind, 'UPSERT INTO data (id, value) VALUES (2u, "after-promotion");')
            rows = self.execute(kind, "SELECT id, value FROM data ORDER BY id;")[0].rows
            assert [(row.id, row.value) for row in rows] == [(1, "before-promotion"), (2, "after-promotion")]
            self.cluster.alter_database(f"/Root/{kind}", storage_units_to_add={kind: 1})
        self.check_pools(2)
