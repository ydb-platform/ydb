LIBRARY()

SRCS(
    group_members.h
    group_members.cpp
    users.h
    users.cpp
    groups.h
    groups.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/base
    ydb/core/kqp/runtime
    ydb/core/sys_view/common
)

YQL_LAST_ABI_VERSION()

END()
