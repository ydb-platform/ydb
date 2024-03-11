PROGRAM()

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/pop_count
    library/cpp/streams/bzip2
    ydb/core/blobstorage
    ydb/core/blobstorage/groupinfo
    ydb/core/blobstorage/vdisk/query
)

END()
