LIBRARY()

SRCS(
    quoter_service.cpp
)

PEERDIR(
    library/cpp/actors/core
    ydb/core/base
    ydb/core/protos
    ydb/core/yq/libs/config/protos
    ydb/core/yq/libs/rate_limiter/events
    ydb/core/yq/libs/shared_resources
    ydb/core/yq/libs/ydb
    ydb/library/security
)

YQL_LAST_ABI_VERSION()

END()
