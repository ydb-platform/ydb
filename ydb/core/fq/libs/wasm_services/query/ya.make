LIBRARY()

YQL_LAST_ABI_VERSION()

SRCS(query.cpp)

PEERDIR(
    ydb/core/fq/libs/config/protos
    ydb/core/fq/libs/wasm_services
    ydb/library/actors/core
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/providers/function/gateway
    ydb/library/yql/providers/function/proto
)

END()
