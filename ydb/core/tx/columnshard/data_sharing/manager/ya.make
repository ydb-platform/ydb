LIBRARY()

SRCS(
    sessions.cpp
    shared_blobs.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/data_sharing/source/session
    ydb/core/tx/columnshard/data_sharing/destination/session
)

END()
