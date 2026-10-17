LIBRARY()

# gn: move into parent

PEERDIR(
    ydb/core/blobstorage/vdisk/common
    ydb/core/protos
)

SRCS(
    blobstorage_hullcompdelete.cpp
    blobstorage_hullcompdelete.h
)

END()
