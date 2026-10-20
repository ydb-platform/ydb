PY3TEST()

SIZE(MEDIUM)

TEST_SRCS(
    test_connectivity.py
    test_control_api.py
    test_events.py
    test_extras.py
    test_followups.py
    test_health.py
    test_model.py
    test_proto_contract.py
    test_replica.py
    test_replication.py
    test_services_grpc.py
    test_shim.py
    test_supervisor_unit.py
    test_topology.py
    test_validator.py
)

PEERDIR(
    ydb/apps/bridge_skipper/ydbd_mock/lib
)

END()
