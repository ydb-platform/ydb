LIBRARY()

SRCS(
<<<<<<< HEAD
    base_service.h
    base.h
=======
    base.cpp
    path_aliasing.cpp
    base_service.h
    base.h
    request_paths.h
    http_database_access_verdict.h
>>>>>>> ed1f2be23f6 ([Relative paths 1/5] Add usage metrics and feature flag (#54693))
)

PEERDIR(
    ydb/library/grpc/server
    library/cpp/string_utils/quote
    ydb/core/base
    ydb/core/grpc_services/counters
    ydb/core/grpc_streaming
    ydb/core/jaeger_tracing
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/resources
    yql/essentials/public/issue
)

YQL_LAST_ABI_VERSION()

END()
