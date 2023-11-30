LIBRARY()

PEERDIR(
    ydb/library/actors/core
    library/cpp/digest/crc32c
    library/cpp/pop_count
    ydb/core/base
    ydb/core/base/services
    ydb/core/blobstorage/crypto
    ydb/core/blobstorage/vdisk/ingress
    ydb/core/protos
)

SRCS(
    blobstorage_groupinfo_blobmap.cpp
    blobstorage_groupinfo_blobmap.h
    blobstorage_groupinfo.cpp
    blobstorage_groupinfo.h
    blobstorage_groupinfo_iter.h
    blobstorage_groupinfo_partlayout.cpp
    blobstorage_groupinfo_partlayout.h
    blobstorage_groupinfo_sets.h
    defs.h
)

END()

RECURSE_FOR_TESTS(
    ut
)
