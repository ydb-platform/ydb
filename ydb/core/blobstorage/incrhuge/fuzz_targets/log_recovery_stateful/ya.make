FUZZ()

SIZE(LARGE)

TAG(
    ya:fat
)

SRCS(
    main.cpp
)

PEERDIR(
    ydb/core/blobstorage/base
    ydb/core/blobstorage/groupinfo
    ydb/core/blobstorage/incrhuge
    ydb/core/protos
)

END()
