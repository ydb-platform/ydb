LIBRARY()

SRCS(
    manager.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/data_sharing/manager
    ydb/core/tx/columnshard/blobs_action/bs
    ydb/core/tx/columnshard/blobs_action/tier
)

END()
