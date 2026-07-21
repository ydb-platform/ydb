FUZZ()

SIZE(LARGE)

TAG(
    ya:fat
)

PEERDIR(
    ydb/core/blobstorage/backpressure
    ydb/core/testlib/actors
)

SRCS(
    main.cpp
)

END()
