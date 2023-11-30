LIBRARY()

SRCS(
    rate_limiter_control_plane_service.cpp
    update_limit_actor.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/fq/libs/config/protos
    ydb/core/fq/libs/events
    ydb/core/fq/libs/quota_manager/events
    ydb/core/fq/libs/rate_limiter/events
    ydb/core/fq/libs/rate_limiter/utils
    ydb/core/fq/libs/shared_resources
    ydb/core/fq/libs/ydb
    ydb/core/protos
    ydb/library/security
)

YQL_LAST_ABI_VERSION()

END()
