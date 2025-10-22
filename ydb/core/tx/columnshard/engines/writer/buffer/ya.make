LIBRARY()

SRCS(
    actor2.cpp
    events.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/protos
    ydb/core/tablet_flat
    yql/essentials/core/expr_nodes
    ydb/library/actors/testlib/common
    ydb/core/tx/columnshard/data_sharing/protos
    ydb/core/tx/columnshard/blobs_action/protos
)

END()
