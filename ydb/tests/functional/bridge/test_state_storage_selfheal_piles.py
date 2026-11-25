import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from state_storage_selfheal import BridgeStateStorageSelfHeal, CONFIG_NAMES, groups, placement, replicas
from test_three_piles import DISCONNECTED


@pytest.mark.parametrize("failed_count", [1, 2], ids=["reassign", "full-reconfiguration"])
class TestBridgeStateStorageSelfHealPileTransitions(BridgeStateStorageSelfHeal):
    def wait_started(self, pile, victims, failed_count):
        pile_id = self.bridge.pile_names.index(pile) + 1

        def started():
            config = self.require_snapshot()
            for name in CONFIG_NAMES:
                current = groups(config, name, pile_id)
                if failed_count == 1:
                    assert any(
                        ring.get("IsDisabled", False) and set(ring["Node"]).intersection(victims)
                        for group in current for ring in group["Ring"]
                    ), (name, current)
                else:
                    assert len(current) == 2, (name, current)
                    assert any(group.get("WriteOnly", False) for group in current), (name, current)
            return config

        return self.wait(started, f"first selfheal phase on {pile}")

    def assert_frozen_holders(self, snapshots, *piles):
        for pile in piles:
            pile_id = self.bridge.pile_names.index(pile) + 1
            for _, config in snapshots:
                for name in CONFIG_NAMES:
                    expected = placement(groups(self.initial, name, pile_id)[0])
                    assert all(placement(group) == expected for group in groups(config, name, pile_id)), (
                        pile, name, config,
                    )

    def observe_another_attempt(self):
        deadline = time.monotonic() + self.relax_seconds + self.step_seconds
        while time.monotonic() < deadline:
            self.check_observer()
            time.sleep(0.5)

    def start_pile_except(self, pile, victims):
        with ThreadPoolExecutor(max_workers=self.nodes_per_pile) as executor:
            list(executor.map(
                lambda node: node.start(),
                (node for node in self.bridge.pile_nodes[pile] if node.node_id not in victims),
            ))

    def test_promotion_during_selfheal(self, failed_count):
        victims = self.choose_victims("r2", failed_count)
        with self.observe():
            self.stop_nodes(victims)
            self.wait_started("r2", victims, failed_count)
            self.bridge.promote("r3")

            def promoted():
                config = self.require_snapshot()
                assert all(
                    group.get("PileState") == "PRIMARY"
                    for name in CONFIG_NAMES for group in groups(config, name, 3)
                ), config

            # Promotion starts after observing the first repair phase. Its final
            # state may be observed after repair has finished on a busy runner.
            self.wait(promoted, "State Storage groups reflecting r3 as PRIMARY")
            after = self.wait_healed(victims)
            self.bridge.write_data()
        if failed_count == 2:
            self.assert_full_reconfiguration(self.initial, after)
        else:
            for name in CONFIG_NAMES:
                old = groups(self.initial, name, 2)[0]
                new = groups(after, name, 2)[0]
                assert old.get("RingGroupActorIdOffset", 0) == new.get("RingGroupActorIdOffset", 0)
                assert replicas(old) - replicas(new) == set(victims)
        self.wait_converged(after)
        self.bridge.promote("r1")
        self.bridge.write_data()

    def test_primary_failure_during_selfheal(self, failed_count):
        victims = self.choose_victims("r2", failed_count)
        with self.observe():
            self.stop_nodes(victims)
            self.wait_started("r2", victims, failed_count)
            self.bridge.stop_pile("r1")
            self.bridge.disconnect("r1", new_primary="r3")
            after = self.wait_healed(victims)
            self.bridge.write_data()
            self.observe_another_attempt()
            self.assert_frozen_holders(self.snapshots, "r1")
        self.wait_converged(after)
        self.bridge.start_pile("r1")
        self.bridge.rejoin("r1")
        self.bridge.promote("r1")
        self.bridge.write_data()

    def test_disconnected_pile_waits_for_rejoin(self, failed_count):
        frozen = self.choose_victims("r2", failed_count)
        repairable = self.choose_victims("r1", failed_count)
        self.bridge.disconnect("r2")
        with self.observe():
            self.stop_nodes(frozen + repairable)
            self.wait_bad_nodes(frozen + repairable)
            self.wait_healed(repairable)
            self.bridge.write_data()
            self.observe_another_attempt()
            self.assert_frozen_holders(self.snapshots, "r2")
        # The failed holders remain down. Rejoining the pile, rather than
        # restarting those holders, must make its healthy spares eligible.
        with self.observe():
            self.bridge.rejoin("r2")
            after = self.wait_healed(frozen + repairable)
        self.wait_converged(after)
        self.bridge.keep_only("r2")
        self.bridge.write_data()

    def test_two_offline_piles_and_partial_recovery(self, failed_count):
        self.control_node = self.bridge.pile_nodes["r1"][-1]
        frozen = {pile: self.choose_victims(pile, failed_count) for pile in ("r2", "r3")}
        with self.observe():
            with ThreadPoolExecutor(max_workers=2) as executor:
                list(executor.map(self.bridge.stop_pile, ("r2", "r3")))
            self.bridge.update([("r2", DISCONNECTED), ("r3", DISCONNECTED)], via="r1")
            self.bridge.states.update({"r2": DISCONNECTED, "r3": DISCONNECTED})
            self.bridge.wait_states()
            self.bridge.write_data()
            victims = self.choose_victims("r1", failed_count)
            self.stop_nodes(victims)
            self.wait_healed(victims)
            self.bridge.write_data()
            self.observe_another_attempt()
            self.assert_frozen_holders(self.snapshots, "r2", "r3")
        # Bring each pile back with failed replica holders still down. This
        # checks that repairs resume on return and use that pile's spare nodes.
        for pile in ("r2", "r3"):
            with self.observe():
                self.start_pile_except(pile, frozen[pile])
                self.bridge.rejoin(pile)
                self.wait_healed(victims + frozen[pile])
                if pile == "r2":
                    self.assert_frozen_holders(self.snapshots, "r3")
                self.bridge.write_data()
        self.bridge.keep_only("r3")
        self.bridge.execute("CREATE TABLE `/Root/after_pile_recovery` (id Uint64, PRIMARY KEY(id));", scheme=True)
        self.bridge.write_data()
