PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)

PEERDIR(
    contrib/python/grpcio
    library/python/testing/yatest_common
    library/python/resource
    ydb/udfs/wasm/profile/contract
    ydb/udfs/wasm/echo/contract
    ydb/udfs/wasm/profile/proto/schema
)

TEST_SRCS(test_query.py)
PY_SRCS(conftest.py)

DATA(
    arcadia/ydb/udfs/wasm/profile/ut/data/profile.wasm
    arcadia/ydb/udfs/wasm/echo/ut/data/echo.wasm
)

SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

END()
