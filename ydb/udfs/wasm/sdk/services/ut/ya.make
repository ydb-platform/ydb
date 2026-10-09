PY3TEST()

TEST_SRCS(test_generate.py)

PEERDIR(
    ydb/udfs/wasm/sdk/services
    library/python/testing/yatest_common
)

DATA(
    arcadia/ydb/udfs/wasm/profile/service.json
    arcadia/ydb/udfs/wasm/echo/service.json
)

END()
