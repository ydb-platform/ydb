LIBRARY()

SRCS(
    abstract.cpp
)

PEERDIR(
    ydb/core/tablet_flat
    ydb/core/tx/columnshard/blobs_action/abstract
    ydb/core/tx/columnshard/resource_subscriber
)

END()
