"""Guards against upstream proto changes that the mock would silently mishandle.

An unreviewed enum value or a moved field makes one of these tests fail. Known
unsupported states are acknowledged explicitly and covered by rejection tests.
"""
import unittest

from google.protobuf import json_format

from ydb.public.api.grpc import ydb_config_v1_pb2_grpc
from ydb.public.api.grpc import ydb_discovery_v1_pb2_grpc
from ydb.public.api.grpc import ydb_monitoring_v1_pb2_grpc
from ydb.public.api.grpc.draft import ydb_bridge_v1_pb2_grpc
from ydb.public.api.protos import ydb_bridge_common_pb2
from ydb.public.api.protos import ydb_config_pb2
from ydb.public.api.protos import ydb_discovery_pb2
from ydb.public.api.protos import ydb_monitoring_pb2
from ydb.public.api.protos import ydb_operation_pb2
from ydb.public.api.protos import ydb_status_codes_pb2
from ydb.public.api.protos.draft import ydb_bridge_pb2

PileState = ydb_bridge_common_pb2.PileState
StatusIds = ydb_status_codes_pb2.StatusIds


class PileStateContractTest(unittest.TestCase):
    def test_known_public_state_values(self):
        actual = dict(PileState.State.items())
        expected = {
            "UNSPECIFIED": 0,
            "PRIMARY": 1,
            "PROMOTED": 2,
            "SYNCHRONIZED": 3,
            "NOT_SYNCHRONIZED": 4,
            "SUSPENDED": 5,
            "DISCONNECTED": 6,
            "INACTIVE": 7,
        }
        self.assertEqual(actual, expected)

    def test_pile_state_fields(self):
        msg = PileState(pile_name="p", state=PileState.PRIMARY)
        self.assertEqual(msg.pile_name, "p")
        self.assertEqual(msg.state, PileState.PRIMARY)


class StatusContractTest(unittest.TestCase):
    def test_status_names_used_by_the_mock(self):
        for name in (
            "SUCCESS", "BAD_REQUEST", "UNAUTHORIZED", "INTERNAL_ERROR", "UNAVAILABLE",
            "TIMEOUT", "PRECONDITION_FAILED", "NOT_FOUND", "UNSUPPORTED", "GENERIC_ERROR",
        ):
            self.assertIn(name, StatusIds.StatusCode.keys(), name)

    def test_success_is_400000(self):
        self.assertEqual(StatusIds.SUCCESS, 400000)


class MonitoringContractTest(unittest.TestCase):
    def test_self_check_results(self):
        for name in ("UNSPECIFIED", "GOOD", "DEGRADED", "MAINTENANCE_REQUIRED", "EMERGENCY"):
            self.assertIn(name, ydb_monitoring_pb2.SelfCheck.Result.keys(), name)

    def test_status_flags(self):
        for name in ("GREY", "GREEN", "BLUE", "YELLOW", "ORANGE", "RED"):
            self.assertIn(name, ydb_monitoring_pb2.StatusFlag.Status.keys(), name)

    def test_issue_log_pile_path_resolves(self):
        issue = ydb_monitoring_pb2.IssueLog(
            id="1-bridge-group-pile2-storage",
            type="BRIDGE_GROUP",
            status=ydb_monitoring_pb2.StatusFlag.RED,
            level=5,
            message="storage",
        )
        issue.location.storage.pool.name = "/Root:mock"
        issue.location.storage.pool.group.id.append("42")
        issue.location.storage.pool.group.pile.name = "pile2"
        result = ydb_monitoring_pb2.SelfCheckResult(
            self_check_result=ydb_monitoring_pb2.SelfCheck.EMERGENCY,
            issue_log=[issue],
        )
        result.location.pile.name = "pile1"
        as_dict = json_format.MessageToDict(result, preserving_proto_field_name=True)
        self.assertEqual(as_dict["self_check_result"], "EMERGENCY")
        got = as_dict["issue_log"][0]
        self.assertEqual(got["type"], "BRIDGE_GROUP")
        self.assertEqual(got["status"], "RED")
        self.assertEqual(got["location"]["storage"]["pool"]["group"]["pile"]["name"], "pile2")
        self.assertEqual(as_dict["location"]["pile"]["name"], "pile1")


class MessageShapeContractTest(unittest.TestCase):
    def test_operation_envelope_fields(self):
        op = ydb_operation_pb2.Operation(ready=True, status=StatusIds.SUCCESS)
        result = ydb_bridge_pb2.GetClusterStateResult(generation=3)
        op.result.Pack(result)
        unpacked = ydb_bridge_pb2.GetClusterStateResult()
        self.assertTrue(op.result.Unpack(unpacked))
        self.assertEqual(unpacked.generation, 3)
        issue = op.issues.add()
        issue.message = "x"
        issue.severity = 1

    def test_bridge_request_fields(self):
        req = ydb_bridge_pb2.UpdateClusterStateRequest()
        req.updates.add(pile_name="a", state=PileState.DISCONNECTED)
        req.quorum_piles.append("b")
        self.assertEqual(req.updates[0].state, PileState.DISCONNECTED)

    def test_discovery_endpoint_fields(self):
        info = ydb_discovery_pb2.EndpointInfo(
            address="127.0.0.1", port=1, node_id=2, location="dc", bridge_pile_name="pile1", ssl=False)
        result = ydb_discovery_pb2.ListEndpointsResult(endpoints=[info], self_location="dc")
        result.pile_states.add(pile_name="pile1", state=PileState.PRIMARY)
        self.assertEqual(result.endpoints[0].bridge_pile_name, "pile1")

    def test_fetch_config_entry_identity(self):
        entry = ydb_config_pb2.ConfigEntry(config="a: 1")
        entry.identity.version = 1
        entry.identity.cluster = "c"
        entry.identity.main.SetInParent()
        self.assertEqual(entry.identity.WhichOneof("type"), "main")


class ServiceContractTest(unittest.TestCase):
    def test_servicers_have_the_rpcs_the_mock_implements(self):
        for cls, names in (
            (ydb_discovery_v1_pb2_grpc.DiscoveryServiceServicer, ["ListEndpoints"]),
            (ydb_config_v1_pb2_grpc.ConfigServiceServicer, ["FetchConfig"]),
            (ydb_monitoring_v1_pb2_grpc.MonitoringServiceServicer, ["SelfCheck"]),
            (ydb_bridge_v1_pb2_grpc.BridgeServiceServicer,
             ["GetClusterState", "UpdateClusterState", "ConvertToBridge", "AddPile", "RemovePile"]),
        ):
            for name in names:
                self.assertTrue(callable(getattr(cls, name, None)), "%s.%s" % (cls.__name__, name))

    def test_registration_functions_exist(self):
        for mod, fn in (
            (ydb_discovery_v1_pb2_grpc, "add_DiscoveryServiceServicer_to_server"),
            (ydb_config_v1_pb2_grpc, "add_ConfigServiceServicer_to_server"),
            (ydb_monitoring_v1_pb2_grpc, "add_MonitoringServiceServicer_to_server"),
            (ydb_bridge_v1_pb2_grpc, "add_BridgeServiceServicer_to_server"),
        ):
            self.assertTrue(callable(getattr(mod, fn, None)), fn)

    def test_bridge_service_has_no_history_rpc(self):
        # GetClusterHistory* messages exist, but there is no such RPC. Do not mock one.
        self.assertFalse(hasattr(ydb_bridge_v1_pb2_grpc.BridgeServiceServicer, "GetClusterHistory"))


if __name__ == "__main__":
    unittest.main()
