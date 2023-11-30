LIBRARY()

SRCS(
    control_plane_config.cpp
)

PEERDIR(
    ydb/library/actors/core
    library/cpp/lwtrace/mon
    library/cpp/monlib/service/pages
    ydb/core/base
    ydb/core/fq/libs/common
    ydb/core/fq/libs/config
    ydb/core/fq/libs/control_plane_config/events
    ydb/core/fq/libs/quota_manager
    ydb/core/fq/libs/quota_manager/events
    ydb/core/fq/libs/rate_limiter/events
    ydb/core/fq/libs/ydb
    ydb/core/mon
    ydb/library/db_pool
    ydb/library/security
    ydb/library/protobuf_printer
    ydb/library/yql/public/issue
    ydb/public/sdk/cpp/client/ydb_scheme
    ydb/public/sdk/cpp/client/ydb_value
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    events
)
