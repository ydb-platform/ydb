LIBRARY()

SRCS(
    blobstorage_pdisk_metadata.h
    blobstorage_pdisk_metadata.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/core/blobstorage/crypto
    ydb/core/blobstorage/nodewarden
    ydb/core/blobstorage/pdisk
    ydb/library/actors/core
    ydb/library/keys
)

END()
