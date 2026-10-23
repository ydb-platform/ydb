import pytest
import requests

from state_storage_selfheal import BridgeStateStorageSelfHeal, CONFIG_NAMES, groups, is_final
from ydb.tests.library.common.types import TabletTypes


class TestBridgeSelfHealRootRestart(BridgeStateStorageSelfHeal):
    def cms_incarnation(self):
        node = self.control_node
        response = requests.get(
            f"http://{node.host}:{node.mon_port}/viewer/json/tabletinfo", params={"enums": "true"}, timeout=15,
        )
        response.raise_for_status()
        tablets = [
            tablet for tablet in response.json().get("TabletStateInfo", [])
            if int(tablet["TabletId"]) == TabletTypes.CMS.tablet_id_for(0) and tablet["State"] == "Active"
        ]
        assert len(tablets) == 1, tablets
        return int(tablets[0]["NodeId"]), int(tablets[0]["Generation"])

    def prepare_separate_root_and_sentinel(self):
        for _ in range(3):
            scepter = self.invoke({"GetStateStorageConfig": {}})["Scepter"]
            cms = self.wait(self.cms_incarnation, "active CMS tablet")
            if scepter["NodeId"] != cms[0]:
                return scepter, cms
            # Preparation only: elect another root before injecting failures.
            # The actual regression must leave the existing Sentinel alive.
            root = scepter["NodeId"]
            self.control_node = self.bridge.cluster.nodes[next(
                node for node in range(self.bridge.nodes_count, 0, -1) if node != root
            )]
            self.bridge.cluster.nodes[root].stop()

            def elected():
                assert self.invoke({"GetStateStorageConfig": {}})["Scepter"]["NodeId"] != root

            self.wait(elected, "root election away from CMS")
            self.bridge.cluster.nodes[root].start()

            def ready():
                config = self.read_config(nodes_state=True)
                assert len(config.get("NodesState", [])) == self.bridge.nodes_count
                assert all(node["State"] == 0 for node in config["NodesState"])
                assert is_final(config), config
                self.assert_pile_locality(config)
                return config

            self.initial = self.wait(ready, "healthy cluster after root election")
        raise AssertionError("could not separate the distconf root from CMS")

    @pytest.mark.parametrize("failed_count, phase", [(1, "disabled"), (2, "introduced"), (2, "readwrite"), (2, "retiring")],
                             ids=["reassign-disabled", "full-introduced", "full-readwrite", "full-retiring"])
    def test_root_restart_during_selfheal(self, failed_count, phase):
        old_scepter, cms_before = self.prepare_separate_root_and_sentinel()
        root = old_scepter["NodeId"]
        root_pile = self.bridge.pile_names[self.node_piles[root] - 1]
        victim_pile = next(pile for pile in self.bridge.pile_names if pile != root_pile)
        victims = [node for node in self.choose_victims(victim_pile, failed_count + 1) if node != cms_before[0]][:failed_count]
        assert len(victims) == failed_count
        self.control_node = self.bridge.cluster.nodes[next(
            node for node in range(self.bridge.nodes_count, 0, -1) if node != root and node not in victims
        )]
        with self.observe():
            self.stop_nodes(victims)

            def introduced():
                config = self.require_snapshot()
                for name in CONFIG_NAMES:
                    for pile_id in range(1, 4):
                        versions = groups(config, name, pile_id)
                        old = groups(self.initial, name, pile_id)[0]
                        if failed_count == 1:
                            assert len(versions) == 1, (name, versions)
                            if pile_id == self.node_piles[victims[0]]:
                                assert any(ring.get("IsDisabled", False) and victims[0] in ring["Node"]
                                           for ring in versions[0]["Ring"]), (name, versions)
                        else:
                            assert len(versions) == 2, (name, versions)
                            old_index = 1 if phase == "retiring" else 0
                            assert versions[old_index].get("RingGroupActorIdOffset", 0) == old.get("RingGroupActorIdOffset", 0)
                            assert versions[1 - old_index].get("RingGroupActorIdOffset", 0) != old.get("RingGroupActorIdOffset", 0)
                            assert not versions[0].get("WriteOnly", False), versions
                            assert versions[1].get("WriteOnly", False) == (phase != "readwrite"), versions

            self.wait(introduced, f"selfheal phase {phase} before root restart")
            old_scepter = self.invoke({"GetStateStorageConfig": {}})["Scepter"]
            root = old_scepter["NodeId"]
            assert root != cms_before[0], "root moved onto CMS before fault injection"
            if self.control_node.node_id == root:
                self.control_node = self.bridge.cluster.nodes[next(
                    node for node in range(self.bridge.nodes_count, 0, -1) if node != root and node not in victims
                )]
            # KiKiMRNode.kill performs an abrupt process kill followed by start.
            self.bridge.cluster.nodes[root].kill()
            after = self.wait_healed(victims)
        assert self.invoke({"GetStateStorageConfig": {}})["Scepter"] != old_scepter
        assert self.cms_incarnation() == cms_before, "CMS/Sentinel restarted during the root-only failure"
        self.wait_bad_nodes(victims)
        states = {node["NodeId"]: node["State"] for node in self.read_config(nodes_state=True)["NodesState"]}
        assert len(states) == self.bridge.nodes_count, states
        assert all(states[node] == 4 for node in victims), states
        self.wait_converged(after)
        self.bridge.execute("CREATE TABLE `/Root/after_root_restart` (id Uint64, PRIMARY KEY(id));", scheme=True)
        self.bridge.write_data()
