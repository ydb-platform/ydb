PY3TEST()

SIZE(MEDIUM)

FORK_SUBTESTS()
# Partition subtests into parallel chunks; several scenarios can share a chunk.
# Keep their required >=20-second BSC warmup in mind when changing the split.
SPLIT_FACTOR(10)

TEST_SRCS(
    test_e2e.py
    test_ports.py
    test_standalone.py
)

DEPENDS(
    ydb/apps/bridge_skipper/ydbd_mock
    ydb/apps/bridge_skipper
)

PEERDIR(
    library/python/port_manager
    library/python/testing/yatest_common
    ydb/apps/bridge_skipper/acceptance/lib
)

END()
