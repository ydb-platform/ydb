LIBRARY()

SRCS(
    config.cpp
    control_plane_proxy.cpp
    probes.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/base
    ydb/core/fq/libs/actors
    ydb/core/fq/libs/actors/logging
    ydb/core/fq/libs/compute/ydb
    ydb/core/fq/libs/compute/ydb/control_plane
    ydb/core/fq/libs/control_plane_config
    ydb/core/fq/libs/control_plane_proxy/actors
    ydb/core/fq/libs/control_plane_proxy/events
    ydb/core/fq/libs/control_plane_storage
    ydb/core/fq/libs/rate_limiter/events
    ydb/core/fq/libs/result_formatter
    ydb/core/mon
    ydb/library/folder_service
    ydb/library/security
    ydb/library/ycloud/api
    ydb/library/ycloud/impl
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    actors
    events
    utils
)

RECURSE_FOR_TESTS(
    ut
)
