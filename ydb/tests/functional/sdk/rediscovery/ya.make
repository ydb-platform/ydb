PY3TEST()

SIZE(MEDIUM)
REQUIREMENTS(cpu:4 ram:12)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(
    test_eds_cutover.py
    test_eds_server.py
)
PY_SRCS(eds_balancer.py)

DEPENDS(
    ydb/tests/functional/sdk/rediscovery/clients/cpp
)

PEERDIR(
    contrib/python/grpcio
    ydb/public/api/grpc
    ydb/public/api/grpc/draft/xds
    ydb/tests/library
    ydb/tests/oss/ydb_sdk_import
)

END()

RECURSE(clients/cpp)
