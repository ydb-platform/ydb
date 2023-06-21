LIBRARY()

SRCS(
    compacted_blob_constructor.cpp
    indexed_blob_constructor.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/tablet_flat/protos
    ydb/core/blobstorage/vdisk/protos
    ydb/core/tablet_flat
    ydb/core/formats/arrow
    

    library/cpp/actors/core
)

YQL_LAST_ABI_VERSION()

END()
