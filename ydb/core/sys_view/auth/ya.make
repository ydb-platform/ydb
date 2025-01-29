LIBRARY()

SRCS(
    group_members.cpp
    group_members.h
    groups.cpp
    groups.h
    owners.cpp
    owners.h
    permissions.cpp
    permissions.h
    users.cpp
    users.h
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/base
    ydb/core/kqp/runtime
    ydb/core/sys_view/common
)

YQL_LAST_ABI_VERSION()

END()
