LIBRARY()

PEERDIR(
    ydb/library/actors/core
    ydb/core/blobstorage/vdisk/common
)

SRCS(
    chunk_keeper_events.cpp
)

END()
