LIBRARY()

SRCS(
    abstract.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/public
    ydb/core/tx/columnshard/common
    ydb/core/tx/tiering/tier
    ydb/core/tx/columnshard/blobs_action/protos
    ydb/core/tx/columnshard/data_sharing/protos
    ydb/public/sdk/cpp/src/client/resources
    yql/essentials/core/expr_nodes
)

END()
