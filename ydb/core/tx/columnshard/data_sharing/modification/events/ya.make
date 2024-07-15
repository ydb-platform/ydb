LIBRARY()

SRCS(
    change_owning.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/blobs_action/protos
    ydb/core/tx/columnshard/data_sharing/protos
    ydb/library/actors/core
    ydb/core/tx/datashard
)

END()
