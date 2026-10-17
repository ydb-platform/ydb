UNITTEST_FOR(ydb/core/kqp)

SIZE(MEDIUM)
FORK_SUBTESTS()

SRCS(kqp_wasm_services_ut.cpp metadata_ut.cpp)

PEERDIR(
    ydb/core/kqp/wasm_services
    ydb/library/yql/providers/function/provider
    library/cpp/http/server
    library/cpp/resource
    library/cpp/testing/common
    ydb/core/kqp/ut/common
    ydb/services/udf_store/wasm
    yql/essentials/sql/pg_dummy
)

RESOURCE(${ARCADIA_ROOT}/ydb/udfs/wasm/echo/ut/data/echo.wasm /echo.wasm)
RESOURCE(${ARCADIA_ROOT}/ydb/udfs/wasm/echo/manifest.json /echo.manifest)

YQL_LAST_ABI_VERSION()

END()
