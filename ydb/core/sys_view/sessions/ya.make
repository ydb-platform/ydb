YQL_LIBRARY()

# gn: move into parent

SRCS(
    sessions.h
    sessions.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/base
    ydb/core/kqp/runtime
    ydb/core/sys_view/common
)
# gn: peerdir ydb/core/mind:tenant_node_enumeration

END()
