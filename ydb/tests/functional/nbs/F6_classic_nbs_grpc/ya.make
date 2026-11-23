PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/functional/nbs/suite.inc)

PEERDIR(
    contrib/python/grpcio
    ydb/core/nbs/nbs1_compat_api/cloud/blockstore/public/api/protos
)

PY_SRCS(
    grpc_client.py
)

TEST_SRCS(
    conftest.py
    test_grpc.py
)

END()
