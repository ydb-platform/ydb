UNITTEST_FOR(ydb/core/blobstorage/vdisk/hulldb/barriers)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/base
    ydb/core/blobstorage/vdisk/common
)

SRCS(
    barriers_tree_ut.cpp
)

END()
