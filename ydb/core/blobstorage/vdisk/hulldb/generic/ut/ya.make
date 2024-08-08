UNITTEST_FOR(ydb/core/blobstorage/vdisk/hulldb/generic)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/base
    ydb/core/blobstorage/vdisk/common
    ydb/core/blobstorage/vdisk/hulldb
)

SRCS(
    hullds_sst_it_all_ut.cpp
    blobstorage_hullwritesst_ut.cpp
)

END()
