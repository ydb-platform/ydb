# -*- coding: utf-8 -*-
import pytest

from common import BridgeKiKiMRTest

from ydb.public.api.protos.ydb_bridge_common_pb2 import PileState
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds


class TestBridgeBasic(BridgeKiKiMRTest):

    def test_update_and_get_cluster_state(self):
        initial_result = self.get_cluster_state(self.bridge_client)
        self.check_states(initial_result, {"r1": PileState.PRIMARY, "r2": PileState.SYNCHRONIZED})

        updates = [
            PileState(pile_name="r2", state=PileState.PROMOTE),
        ]
        self.update_cluster_state(self.bridge_client, updates)
        self.wait_for_cluster_state(self.bridge_client, {"r1": PileState.PRIMARY, "r2": PileState.PROMOTE})

    # TODO: uncomment when we stabilize this scenario
    # def test_failover(self):
    #     initial_result = get_cluster_state(self.bridge_client)
    #     self.check_states(initial_result, {"r1": PileState.PRIMARY, "r2": PileState.SYNCHRONIZED})

    #     update_cluster_state(self.bridge_client, [
    #         PileState(pile_name="r1", state=PileState.DISCONNECTED),
    #         PileState(pile_name="r2", state=PileState.PRIMARY),
    #     ])
    #     wait_for_cluster_state(self.bridge_client, {"r1": PileState.DISCONNECTED, "r2": PileState.PRIMARY})

    #     update_cluster_state(self.secondary_bridge_client, [
    #         PileState(pile_name="r1", state=PileState.NOT_SYNCHRONIZED),
    #     ])
    #     wait_for_cluster_state(self.secondary_bridge_client, {"r1": PileState.NOT_SYNCHRONIZED, "r2": PileState.PRIMARY})


class TestBridgeValidation(BridgeKiKiMRTest):

    @pytest.mark.parametrize(
        "updates, test_name",
        [
            (
                [],
                "no_updates"
            ),
            (
                [
                    PileState(pile_name="r1", state=PileState.PRIMARY),
                    PileState(pile_name="r2", state=PileState.PRIMARY),
                ],
                "multiple_primary_piles_in_request"
            ),
            (
                [
                    PileState(pile_name="r1", state=PileState.SYNCHRONIZED),
                ],
                "no_primary_pile_in_result"
            ),
            (
                [
                    PileState(pile_name="r1", state=PileState.SYNCHRONIZED),
                    PileState(pile_name="r1", state=PileState.PRIMARY),
                ],
                "duplicate_pile_update"
            ),
            (
                [
                    PileState(pile_name="r3", state=PileState.PRIMARY),
                ],
                "invalid_pile_name"
            ),
        ]
    )
    def test_invalid_updates(self, updates, test_name):
        self.logger.info(f"Running validation test: {test_name}")
        self.update_cluster_state(self.bridge_client, updates, StatusIds.BAD_REQUEST)
