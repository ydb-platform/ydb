FUZZ()

SIZE(LARGE)

TAG(
    ya:fat
)

PEERDIR(
    ydb/core/blobstorage/backpressure
    ydb/library/actors/core
)

SRCS(
    main.cpp
)

END()
