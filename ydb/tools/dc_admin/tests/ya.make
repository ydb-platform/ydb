PY3TEST()

TEST_SRCS(
    test_placement.py
    test_server.py
    test_static_recovery.py
    test_workload.py
)

PEERDIR(ydb/tools/dc_admin)

END()
