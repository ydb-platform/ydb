PY3TEST()

SIZE(MEDIUM)
REQUIREMENTS(cpu:4 ram:12)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(
    test_long_poll.py
)
PY_SRCS(long_poll_balancer.py run_long_poll_probe.py)

DEPENDS(
    ydb/tests/functional/sdk/rediscovery/clients/cpp
)

PEERDIR(
    contrib/python/grpcio
    ydb/public/api/grpc
    ydb/tests/library
    ydb/tests/oss/ydb_sdk_import
)

END()

RECURSE(clients/cpp)
