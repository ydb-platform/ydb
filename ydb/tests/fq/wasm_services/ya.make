PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)

PEERDIR(
    contrib/python/grpcio
    library/python/testing/yatest_common
    ydb/core/fq/libs/wasm_services/ut/protos
)

TEST_SRCS(test_query.py)
PY_SRCS(conftest.py)

DATA(arcadia/ydb/core/fq/libs/wasm_services/ut/data/transport_coroutine.wasm)

SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

END()
