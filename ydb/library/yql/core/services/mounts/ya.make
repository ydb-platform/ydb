LIBRARY()

SRCS(
    yql_mounts.h
    yql_mounts.cpp
)

PEERDIR(
    library/cpp/resource
    ydb/library/yql/core/user_data
    ydb/library/yql/core
)

YQL_LAST_ABI_VERSION()

RESOURCE(
    ydb/library/yql/mount/lib/yql/aggregate.yql /lib/yql/aggregate.yql
    ydb/library/yql/mount/lib/yql/window.yql /lib/yql/window.yql
    ydb/library/yql/mount/lib/yql/id.yql /lib/yql/id.yql
    ydb/library/yql/mount/lib/yql/sqr.yql /lib/yql/sqr.yql
    ydb/library/yql/mount/lib/yql/core.yql /lib/yql/core.yql
    ydb/library/yql/mount/lib/yql/walk_folders.yql /lib/yql/walk_folders.yql
)

END()
