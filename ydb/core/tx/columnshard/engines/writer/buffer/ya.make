LIBRARY()

OWNER(
    g:kikimr
)

SRCS(
    actor.cpp
    events.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/protos
    ydb/core/tablet_flat
    ydb/library/yql/core/expr_nodes
    ydb/library/actors/testlib/common
)

END()
