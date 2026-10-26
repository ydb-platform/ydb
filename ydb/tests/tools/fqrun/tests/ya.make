PY3TEST()

TEST_SRCS(test_wasm_services.py)

PEERDIR(
    library/python/testing/yatest_common
    ydb/tests/library
    ydb/tests/tools/fqrun/examples/service
)

DEPENDS(ydb/tests/tools/fqrun)

DATA(
    arcadia/ydb/tests/tools/fqrun/configuration/app_config.conf
    arcadia/ydb/udfs/wasm/profile/ut/data/profile.wasm
    arcadia/ydb/udfs/wasm/profile/manifest.json
    arcadia/ydb/udfs/wasm/echo/ut/data/echo.wasm
    arcadia/ydb/udfs/wasm/echo/manifest.json
)

SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
TIMEOUT(900)
REQUIREMENTS(ram:16)
END()
