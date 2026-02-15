LIBRARY()

PEERDIR(
    contrib/libs/rocksdb
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/core/blobstorage/groupinfo
    ydb/core/blobstorage/pdisk
    ydb/core/blobstorage/vdisk
    ydb/core/blobstorage/vdisk/common
    ydb/core/blobstorage/vdisk/protos
    ydb/core/protos
    ydb/library/actors/protos
)

SRCS(
    rocksdisk.h
    rocksdisk.cpp
    rocksdisk_actor.h
    rocksdisk_actor.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
