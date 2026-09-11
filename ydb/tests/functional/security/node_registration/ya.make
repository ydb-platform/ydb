PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(
    conftest.py
    helpers.py
    test_node_authentication.py
    test_startup_credentials.py
)

PEERDIR(
    contrib/python/cryptography
    contrib/python/grpcio
    library/python/port_manager
    ydb/core/protos
    ydb/public/api/grpc
    ydb/public/api/protos
    ydb/tests/library
)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    REQUIREMENTS(ram:10 cpu:16)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()
