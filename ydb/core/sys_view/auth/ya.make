YQL_LIBRARY()

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
    ydb/core/base
    ydb/core/kqp/common/events
    ydb/core/kqp/runtime/common
    ydb/core/sys_view/common
    ydb/library/actors/core
)

END()
