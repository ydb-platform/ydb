LIBRARY()

OWNER(
    monster
    g:kikimr
)

SRCS(
    counters.cpp
    counters.h
)

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/core/sys_view/service
)

YQL_LAST_ABI_VERSION()

END()
