LIBRARY()

SRCS(
    move_data.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/tx/columnshard/blobs_action/protos
    ydb/core/tx/columnshard/data_sharing/protos
    ydb/core/tx/columnshard/engines/protos
    ydb/library/actors/core
    yql/essentials/core/expr_nodes
)

YQL_LAST_ABI_VERSION()

END()
