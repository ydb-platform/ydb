UNITTEST()

YQL_LAST_ABI_VERSION()

SIZE(MEDIUM)

SRCS(transport_ut.cpp)

PEERDIR(
    library/cpp/http/server
    library/cpp/resource
    library/cpp/testing/common
    library/cpp/testing/unittest
    ydb/core/fq/libs/wasm_services
    ydb/core/fq/libs/wasm_services/ut/protos
    ydb/library/wasm/api
    ydb/library/wasm/engine
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

RESOURCE(data/transport_coroutine.wasm /fq_transport_coroutine.wasm)

END()
