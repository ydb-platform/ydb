LIBRARY()

SRCS(
    sessions.cpp  # gn: into ydb/core/tx/columnshard
    shared_blobs.cpp  # gn: shared_blobs
)
# gn: shared_blobs headers shared_blobs.h

PEERDIR(
    ydb/core/tx/columnshard/data_sharing/source/session
    ydb/core/tx/columnshard/data_sharing/destination/session
)

END()
