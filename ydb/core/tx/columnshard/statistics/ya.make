LIBRARY()

SRCS(
    reporter.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/base
    ydb/core/tx/columnshard/blobs_action/protos
    ydb/core/tx/columnshard/data_sharing/protos
    yql/essentials/core/expr_nodes
)

END()
