from common import BridgeKiKiMRTest

from ydb.public.api.grpc.ydb_discovery_v1_pb2_grpc import DiscoveryServiceStub
from ydb.public.api.protos.ydb_bridge_common_pb2 import PileState
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds

from ydb.public.api.protos import ydb_discovery_pb2 as discovery

import grpc

from hamcrest import assert_that, is_


class TestDiscovery(BridgeKiKiMRTest):
    def list_endpoints(self):
        channel = grpc.insecure_channel(self.cluster.nodes[1].endpoint)
        stub = DiscoveryServiceStub(channel)
        request = discovery.ListEndpointsRequest(database="/Root")
        response = stub.ListEndpoints(request)

        result = discovery.ListEndpointsResult()
        response.operation.result.Unpack(result)
        return result, response.operation.status

    def check_endpoints(self, result):
        assert_that(len(result.endpoints), is_(self.nodes_count))
        expected_pile_names = {node_id: host_info['location']['bridge_pile_name'] for node_id, host_info in enumerate(self.cluster.config.yaml_config["hosts"], 1)}
        for endpoint in result.endpoints:
            assert_that(endpoint.bridge_pile_name, is_(expected_pile_names[endpoint.node_id]))

    def test_basic(self):
        result, status = self.list_endpoints()
        assert_that(status, is_(StatusIds.SUCCESS))
        self.check_endpoints(result)
        self.check_states(result, {
            "r1": PileState.PRIMARY,
            "r2": PileState.SYNCHRONIZED,
        })

    def test_update_piles(self):
        result, status = self.list_endpoints()
        assert_that(status, is_(StatusIds.SUCCESS))
        self.check_endpoints(result)
        self.check_states(result, {
            "r1": PileState.PRIMARY,
            "r2": PileState.SYNCHRONIZED,
        })

        updates = [
            PileState(pile_name="r2", state=PileState.PROMOTE),
        ]
        self.update_cluster_state(self.bridge_client, updates)
        self.wait_for_cluster_state(self.bridge_client, {"r1": PileState.PRIMARY, "r2": PileState.PROMOTE})

        result, status = self.list_endpoints()
        assert_that(status, is_(StatusIds.SUCCESS))
        self.check_endpoints(result)
        self.check_states(result, {
            "r1": PileState.PRIMARY,
            "r2": PileState.PROMOTE,
        })
