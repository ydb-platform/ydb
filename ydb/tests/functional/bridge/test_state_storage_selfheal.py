import time

import pytest

from state_storage_selfheal import BridgeStateStorageSelfHeal, CONFIG_NAMES, groups, placement, replicas


class TestBridgeStateStorageSelfHeal(BridgeStateStorageSelfHeal):
    @pytest.mark.parametrize("pile", ["r1", "r2", "r3"], ids=["primary", "secondary-r2", "secondary-r3"])
    @pytest.mark.parametrize("failed_count", [1, 2], ids=["reassign", "full-reconfiguration"])
    def test_automatic_selfheal(self, pile, failed_count):
        victims = self.choose_victims(pile, failed_count)
        with self.observe():
            self.stop_nodes(victims)
            after = self.wait_healed(victims)
            self.bridge.write_data()

        if failed_count == 1:
            self.assert_reassign(self.initial, after, victims)
        else:
            self.assert_full_reconfiguration(self.initial, after)
        self.wait_converged(after)

        # Exercise a new metadata operation after the repair and process recovery.
        for node_id in victims:
            self.bridge.cluster.nodes[node_id].start()
        self.bridge.execute("CREATE TABLE `/Root/after_selfheal` (id Uint64, PRIMARY KEY(id));", scheme=True)
        self.bridge.write_data()


class TestBridgeStateStorageSelfHealWithoutLocalSpare(BridgeStateStorageSelfHeal):
    # Six surviving nodes still form a configuration quorum in a ten-node pile
    # after two replica holders and both unused hosts have been stopped.
    nodes_per_pile = 10
    # Rack names sort numerically, placing the eight State Storage holders on
    # the static BlobStorage hosts and leaving the last two hosts as spares.
    node_location_overrides = {
        node_id: {"rack": f"rack-{node_id:03d}"}
        for node_id in range(1, nodes_per_pile * 3 + 1)
    }

    @pytest.mark.parametrize("failed_count", [1, 2], ids=["reassign", "full-reconfiguration"])
    def test_does_not_borrow_nodes_from_another_pile(self, failed_count):
        blocked = self.choose_victims("r2", failed_count)
        repairable = self.choose_victims("r1", failed_count)
        occupied = set.union(*(replicas(groups(self.initial, name, 2)[0]) for name in CONFIG_NAMES))
        spares = [node.node_id for node in self.bridge.pile_nodes["r2"] if node.node_id not in occupied]
        assert spares, (occupied, self.initial)
        storage = self.invoke({"QueryConfig": {}})["QueryConfig"]["Config"]
        static_nodes = {
            vdisk["VDiskLocation"]["NodeID"]
            for vdisk in storage["BlobStorageConfig"]["ServiceSet"]["VDisks"]
        }
        # Exhaust State Storage capacity without exceeding the static group
        # failure budget: distconf needs its BlobStorage quorum as well.
        assert not static_nodes.intersection(spares), (spares, static_nodes)
        with self.observe():
            self.stop_nodes(blocked + spares + repairable)
            self.wait_bad_nodes(blocked + spares + repairable)
            assert all(
                not node.is_alive() or node.node_id in occupied
                for node in self.bridge.pile_nodes["r2"]
            )

            def only_repairable_healed():
                config = self.require_snapshot()
                for name in CONFIG_NAMES:
                    assert len(groups(config, name)) == 3, (name, config)
                    primary_group = groups(config, name, 1)[0]
                    assert not replicas(primary_group).intersection(repairable), (name, primary_group)
                    assert not primary_group.get("WriteOnly", False)
                    assert all(not ring.get("IsDisabled", False) for ring in primary_group["Ring"])
                return config

            after = self.wait(only_repairable_healed, "selfheal on r1 while r2 has no local candidate")
            # The stopped hosts can also contain dynamic BlobStorage VDisks.
            # Check the workload once local capacity has been restored below.
            # Keep observing across another opportunity to self-heal. A successful
            # repair on r1 proves the negative assertion is not a stalled Sentinel.
            deadline = time.monotonic() + self.relax_seconds + self.step_seconds
            while time.monotonic() < deadline:
                self.check_observer()
                time.sleep(0.1)
            for _, config in self.snapshots:
                for name in CONFIG_NAMES:
                    for group in groups(config, name, 2):
                        assert placement(group) == placement(groups(self.initial, name, 2)[0]), (name, group)
            if failed_count == 1:
                self.assert_reassign(self.initial, after, repairable)
            else:
                self.assert_full_reconfiguration(self.initial, after)

        # Restore local eligibility through real node recovery, then verify the
        # incomplete placement does not prevent the pile from serving data.
        for node_id in blocked + spares:
            self.bridge.cluster.nodes[node_id].start()
        self.bridge.keep_only("r2")
        self.bridge.write_data()
