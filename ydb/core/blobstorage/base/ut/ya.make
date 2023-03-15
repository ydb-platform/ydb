GTEST()

SIZE(SMALL)

PEERDIR(
    ydb/core/blobstorage/base
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/erasure
    ydb/core/protos
)

SRCS(
    ydb/core/blobstorage/base/batched_vec_ut.cpp
    ydb/core/blobstorage/base/bufferwithgaps_ut.cpp
    ydb/core/blobstorage/base/ptr_ut.cpp
)

END()
