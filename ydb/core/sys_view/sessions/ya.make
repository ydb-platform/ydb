YQL_LIBRARY()

SRCS(
    sessions.h
    sessions.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/kqp/common/events
    ydb/core/kqp/common/simple
    ydb/core/kqp/runtime/common
    ydb/core/sys_view/common
    ydb/library/actors/core
)

END()
