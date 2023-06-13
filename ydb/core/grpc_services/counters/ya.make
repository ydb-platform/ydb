LIBRARY()

SRCS(
    counters.cpp
    counters.h
    proxy_counters.cpp
    proxy_counters.h
)

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/core/sys_view/service
)

YQL_LAST_ABI_VERSION()

END()
