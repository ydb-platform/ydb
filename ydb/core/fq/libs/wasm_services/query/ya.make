LIBRARY()

YQL_LAST_ABI_VERSION()

SRCS(query.cpp module_source.cpp fq_integration.cpp auth.cpp secrets.cpp)

PEERDIR(
    library/cpp/string_utils/base64
    ydb/services/scheme_secret
    library/cpp/json
    ydb/core/fq/libs/common
    ydb/public/lib/udf/manifest
    ydb/public/api/protos
    ydb/core/fq/libs/wasm_services/manifest
    ydb/core/fq/libs/config/protos
    ydb/core/fq/libs/wasm_services
    ydb/library/actors/core
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/providers/function/gateway
    ydb/library/yql/providers/function/proto
)

END()
