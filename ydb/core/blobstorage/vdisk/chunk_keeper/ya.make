LIBRARY()

PEERDIR(
    ydb/library/actors/core
    ydb/core/blobstorage/vdisk/common
)

SRCS(
    chunk_keeper_actor.cpp
    chunk_keeper_data.cpp
    chunk_keeper_events.cpp
)

END()
