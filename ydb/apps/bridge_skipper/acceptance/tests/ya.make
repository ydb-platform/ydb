PY3TEST()

SIZE(MEDIUM)

FORK_SUBTESTS()
# Give each current scenario its own chunk: BSC warmup alone takes >=20 seconds.
# Leave headroom for new cases without regrouping slow scenarios under one timeout.
SPLIT_FACTOR(32)

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
