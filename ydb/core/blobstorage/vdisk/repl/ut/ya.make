UNITTEST_FOR(ydb/core/blobstorage/vdisk/repl)

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
    blobstorage_hullreplwritesst_ut.cpp
    blobstorage_replrecoverymachine_ut.cpp
)

END()
