LIBRARY()

SRCS(
    yql_mounts.h
    yql_mounts.cpp
)

PEERDIR(
    library/cpp/resource
    yql/essentials/core/user_data
    yql/essentials/core
    yql/essentials/utils/log
)

YQL_LAST_ABI_VERSION()

RESOURCE(
    yql/essentials/mount/lib/yql/aggregate.yqls /lib/yql/aggregate.yqls
    yql/essentials/mount/lib/yql/window.yqls /lib/yql/window.yqls
    yql/essentials/mount/lib/yql/id.yqls /lib/yql/id.yqls
    yql/essentials/mount/lib/yql/sqr.yqls /lib/yql/sqr.yqls
    yql/essentials/mount/lib/yql/core.yqls /lib/yql/core.yqls
    yql/essentials/mount/lib/yql/walk_folders.yqls /lib/yql/walk_folders.yqls
)

END()
