UNITTEST_FOR(ydb/core/blobstorage/vdisk/hulldb)

OWNER(g:kikimr)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/base
    ydb/core/blobstorage
    ydb/core/erasure
)

SRCS(
    hullds_cache_block_ut.cpp
)

END()
