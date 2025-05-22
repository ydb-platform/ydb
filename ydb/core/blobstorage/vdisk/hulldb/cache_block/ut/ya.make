UNITTEST_FOR(ydb/core/blobstorage/vdisk/hulldb/cache_block)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/base
    ydb/core/blobstorage
    ydb/core/erasure
)

SRCS(
    cache_block_ut.cpp
)

END()
