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
    ydb/library/services
    ydb/library/yql/providers/common/ut_helpers
    ydb/library/wasm/api
    ydb/library/wasm/engine
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

RESOURCE(data/transport_coroutine.wasm /fq_transport_coroutine.wasm)

END()
