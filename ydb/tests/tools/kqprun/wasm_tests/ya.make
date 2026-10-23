PY3TEST()

TEST_SRCS(test_wasm_services.py)

PEERDIR(
    library/python/testing/yatest_common
    ydb/public/api/grpc
    contrib/python/grpcio
    ydb/public/sdk/python
    ydb/tests/library
    ydb/tests/tools/fqrun/examples/service
)

DEPENDS(ydb/tests/tools/kqprun)

DATA(
    arcadia/ydb/udfs/wasm/profile/ut/data/profile.wasm
    arcadia/ydb/udfs/wasm/profile/manifest.json
)

SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
TIMEOUT(900)
REQUIREMENTS(ram:16)
END()
