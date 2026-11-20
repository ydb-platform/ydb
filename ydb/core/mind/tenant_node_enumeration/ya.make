LIBRARY()

SRCS(
    tenant_node_enumeration.cpp
    tenant_node_enumeration.h
)

PEERDIR(
    ydb/core/base
    ydb/core/mind/events
    ydb/library/actors/core
)

END()
