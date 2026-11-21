# -*- coding: utf-8 -*-
import time

import requests

from common import BridgeKiKiMRTest
from ydb.public.api.protos.ydb_bridge_common_pb2 import PileState


COUNTERS = ("StorageNodes", "AliveStorageNodes", "ComputeNodes", "AliveComputeNodes")


def counters(storage, alive_storage, compute, alive_compute):
    return dict(zip(COUNTERS, (storage, alive_storage, compute, alive_compute)))


class TestViewerPileNodes(BridgeKiKiMRTest):
    database = "/Root/viewer_pile_nodes"
    wait_timeout = 120

    def setup_method(self):
        super().setup_method()
        try:
            self.cluster.create_database(self.database, storage_pool_units_count={"rot": 1})
            self.slots = {
                pile: self.cluster.register_and_start_slots(self.database, bridge_pile_name=pile)[0]
                for pile in self.pile_names
            }
            self.cluster.wait_tenant_up(self.database)
        except Exception:
            self.teardown_method()
            raise

    def get_piles(self, node, **params):
        response = requests.get(
            f"http://{node.host}:{node.mon_port}/viewer/json/cluster",
            params={"timeout": 10000, **params}, timeout=30,
        )
        response.raise_for_status()
        piles = response.json()["BridgeInfo"]["Piles"]
        return {pile["Name"]: {key: pile.get(key) for key in COUNTERS} for pile in piles}, response

    def wait_piles(self, node, expected, **params):
        deadline = time.monotonic() + self.wait_timeout
        while True:
            try:
                actual, response = self.get_piles(node, **params)
                if actual == expected:
                    return response
            except (requests.RequestException, KeyError, ValueError) as error:
                actual = error
            if time.monotonic() >= deadline:
                raise AssertionError(f"Viewer on node {node.node_id} returned {actual}, expected {expected}")
            time.sleep(1)

    def test_pile_node_counts(self):
        nodes_per_pile = self.nodes_count // len(self.pile_names)
        storage_node = self.pile_nodes["r1"][0]
        all_alive = {pile: counters(nodes_per_pile, nodes_per_pile, 1, 1) for pile in self.pile_names}
        self.wait_piles(storage_node, all_alive)
        self.wait_piles(self.slots["r1"], all_alive)

        # Stopped nodes stay known to the nameservice, but do not answer whiteboard requests:
        # they are counted as not alive instead of making the pile counters unknown.
        self.pile_nodes["r2"][-1].stop()
        self.slots["r2"].stop()
        r2_partially_alive = counters(nodes_per_pile, nodes_per_pile - 1, 1, 0)
        self.wait_piles(storage_node, {"r1": all_alive["r1"], "r2": r2_partially_alive})

        self.update_cluster_state(self.bridge_client, [PileState(pile_name="r2", state=PileState.DISCONNECTED)])
        self.wait_for_cluster_state(self.bridge_client, {"r1": PileState.PRIMARY, "r2": PileState.DISCONNECTED}, 60)

        # The disconnected pile still serves its own node statistics.
        self.wait_piles(self.pile_nodes["r2"][0], {"r2": r2_partially_alive}, local_pile_node_stats="true")

        # Interconnect to r2 is forbidden now, and the test cluster serves monitoring over plain HTTP on
        # per-node ports, so the HTTPS fallback cannot reach r2 and its alive counters stay unknown.
        r2_unknown = {"r1": all_alive["r1"], "r2": counters(nodes_per_pile, None, 1, None)}
        self.wait_piles(storage_node, r2_unknown)

        # A compute node redirects the request to a storage node of its own pile, which owns
        # the common storage monitoring port used to reach other piles.
        response = self.wait_piles(self.slots["r1"], r2_unknown)
        assert [r.status_code for r in response.history] == [307], response.history
        assert response.history[0].headers["Location"].startswith("/node/"), response.history[0].headers
