SUBSCRIBER(g:kikimr)
PY3TEST()

TEST_SRCS(
    test_stability.py
)

TIMEOUT(18000)
SIZE(LARGE)
TAG(ya:not_autocheck ya:fat ya:manual)

DATA(
    arcadia/kikimr/ci/stability/resources
)

DEPENDS(
    ydb/tools/simple_queue
    ydb/tools/cfg/bin
    ydb/tests/tools/nemesis/driver
)

PEERDIR(
    ydb/tests/library
)

END()

