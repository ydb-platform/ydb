LIBRARY()

SRCS(
    delete_blobs.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/blobs_action/protos
    ydb/library/actors/core
    ydb/core/tx/datashard
)

END()
