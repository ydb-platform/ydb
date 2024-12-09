LIBRARY()

SRCS(
    yql_mounts.h
    yql_mounts.cpp
)

PEERDIR(
    library/cpp/resource
    yql/essentials/core/user_data
    yql/essentials/core
)

YQL_LAST_ABI_VERSION()

RESOURCE(
    yql/essentials/mount/lib/yql/aggregate.yql /lib/yql/aggregate.yql
    yql/essentials/mount/lib/yql/window.yql /lib/yql/window.yql
    yql/essentials/mount/lib/yql/id.yql /lib/yql/id.yql
    yql/essentials/mount/lib/yql/sqr.yql /lib/yql/sqr.yql
    yql/essentials/mount/lib/yql/core.yql /lib/yql/core.yql
    yql/essentials/mount/lib/yql/walk_folders.yql /lib/yql/walk_folders.yql
)

END()
