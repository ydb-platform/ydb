LIBRARY()

SRCS(
    nodes.h
    nodes.cpp
)

PEERDIR(
    library/cpp/actors/core
    ydb/core/base
    ydb/core/kqp/runtime
    ydb/core/sys_view/common
)

YQL_LAST_ABI_VERSION()

END()
