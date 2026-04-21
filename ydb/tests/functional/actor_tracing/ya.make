PY3TEST()

TEST_SRCS(
    test_actor_tracing.py
)

SIZE(LARGE)
TAG(ya:fat)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

PEERDIR(
    ydb/tests/library
    ydb/public/api/grpc
    ydb/public/api/protos
)

END()
