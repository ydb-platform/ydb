LIBRARY()

SRCS(
    rate_limiter_control_plane_service.cpp
    update_limit_actor.cpp
)

PEERDIR(
    library/cpp/actors/core
    ydb/core/protos
    ydb/core/yq/libs/config/protos
    ydb/core/yq/libs/events
    ydb/core/yq/libs/quota_manager/events
    ydb/core/yq/libs/rate_limiter/events
    ydb/core/yq/libs/rate_limiter/utils
    ydb/core/yq/libs/shared_resources
    ydb/core/yq/libs/ydb
    ydb/library/security
)

YQL_LAST_ABI_VERSION()

END()
