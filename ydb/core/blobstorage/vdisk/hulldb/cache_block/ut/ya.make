UNITTEST_FOR(ydb/core/blobstorage/vdisk/hulldb/cache_block)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

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
