UNITTEST()

YQL_LAST_ABI_VERSION()

SIZE(MEDIUM)

SRCS(transport_ut.cpp)

PEERDIR(
    library/cpp/http/server
    library/cpp/resource
    library/cpp/testing/common
    library/cpp/testing/unittest
    library/cpp/lwtrace/protos
    ydb/core/fq/libs/wasm_services
    ydb/core/fq/libs/wasm_services/query
    ydb/core/fq/libs/wasm_services/ut/protos
    ydb/core/security/certificate_check/test_utils
    ydb/library/actors/http
    ydb/library/actors/testlib
    ydb/library/protobuf_printer
    ydb/library/services
    ydb/library/yql/providers/common/ut_helpers
    ydb/udfs/wasm/profile/contract
    ydb/udfs/wasm/echo/contract
    ydb/library/wasm/api
    ydb/library/wasm/engine
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

RESOURCE(${ARCADIA_ROOT}/ydb/udfs/wasm/profile/ut/data/profile.wasm /fq_transport_coroutine.wasm)
RESOURCE(${ARCADIA_ROOT}/ydb/udfs/wasm/echo/ut/data/echo.wasm /echo_service.wasm)
RESOURCE(${ARCADIA_ROOT}/ydb/udfs/wasm/profile/manifest.json /ydb/udfs/wasm/profile/manifest.json)
RESOURCE(${ARCADIA_ROOT}/ydb/udfs/wasm/echo/manifest.json /ydb/udfs/wasm/echo/manifest.json)

END()
