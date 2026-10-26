LIBRARY()

YQL_LAST_ABI_VERSION()

SRCS(integration.cpp connection.cpp)

PEERDIR(
    ydb/core/external_sources
    ydb/core/fq/libs/wasm_services/query
    ydb/core/kqp/provider
    ydb/library/yql/providers/function/provider
)

END()
