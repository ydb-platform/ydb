LIBRARY()

SRCS(
    blobstorage_proxy_config.h
)

PEERDIR(
    ydb/core/base
    ydb/core/blobstorage/groupinfo
    ydb/core/blobstorage/storagepoolmon
    ydb/library/actors/core
)

END()
