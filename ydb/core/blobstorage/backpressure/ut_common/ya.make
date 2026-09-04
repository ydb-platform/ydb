GTEST()

SIZE(MEDIUM)

PEERDIR(
    ydb/core/base
    ydb/core/blobstorage/backpressure
    ydb/core/testlib/actors
)

SRCS(
    ydb/core/blobstorage/backpressure/common_ut.cpp
)

END()
