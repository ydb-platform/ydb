LIBRARY()

SRCS(
    quoter_service.cpp
)

PEERDIR(
    library/cpp/actors/core
    ydb/core/base
    ydb/core/fq/libs/config/protos
    ydb/core/fq/libs/rate_limiter/events
    ydb/core/fq/libs/shared_resources
    ydb/core/fq/libs/ydb
    ydb/core/protos
    ydb/library/security
)

YQL_LAST_ABI_VERSION()

END()
