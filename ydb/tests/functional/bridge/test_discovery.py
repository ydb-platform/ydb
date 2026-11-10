from common import BridgeKiKiMRTest
from ydb.tests.library.common.wait_for import retry_assertions

from ydb.public.api.grpc.ydb_discovery_v1_pb2_grpc import DiscoveryServiceStub
from ydb.public.api.protos.ydb_bridge_common_pb2 import PileState
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds

from ydb.public.api.protos import ydb_discovery_pb2 as discovery

import grpc

from hamcrest import assert_that, is_


class TestDiscovery(BridgeKiKiMRTest):
    def list_endpoints(self):
        with grpc.insecure_channel(self.cluster.nodes[1].endpoint) as channel:
            stub = DiscoveryServiceStub(channel)
            request = discovery.ListEndpointsRequest(database="/Root")
            response = stub.ListEndpoints(request, timeout=self.bridge_rpc_timeout)

        result = discovery.ListEndpointsResult()
        assert_that(response.operation.result.Unpack(result), str(response))
        return result, response.operation.status

    def check_endpoints(self, result):
        assert_that(len(result.endpoints), is_(self.nodes_count))
        expected_pile_names = {node_id: host_info['location']['bridge_pile_name'] for node_id, host_info in enumerate(self.cluster.config.yaml_config["hosts"], 1)}
        for endpoint in result.endpoints:
            assert_that(endpoint.bridge_pile_name, is_(expected_pile_names[endpoint.node_id]))

    def wait_for_discovery(self, expected_states):
        def check():
            try:
                result, status = self.list_endpoints()
            except grpc.RpcError as error:
                if error.code() not in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                    raise
                raise AssertionError(f"Discovery is not ready: {error}") from error
            assert_that(status, is_(StatusIds.SUCCESS))
            self.check_endpoints(result)
            self.check_states(result, expected_states)

        retry_assertions(check, timeout_seconds=120, step_seconds=0.5)

    def test_basic(self):
        self.wait_for_discovery({
            "r1": PileState.PRIMARY,
            "r2": PileState.SYNCHRONIZED,
        })

    def test_update_piles(self):
        self.wait_for_discovery({
            "r1": PileState.PRIMARY,
            "r2": PileState.SYNCHRONIZED,
        })

        updates = [
            PileState(pile_name="r2", state=PileState.PROMOTED),
        ]
        self.update_cluster_state(self.bridge_client, updates)
        self.wait_for_cluster_state(
            self.bridge_client, {"r1": PileState.PRIMARY, "r2": PileState.PROMOTED}, timeout_seconds=120,
        )

        self.wait_for_discovery({
            "r1": PileState.PRIMARY,
            "r2": PileState.PROMOTED,
        })
