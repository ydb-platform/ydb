UNITTEST_FOR(ydb/core/blobstorage/vdisk/repl)

FORK_SUBTESTS()
SPLIT_FACTOR(5)

SIZE(MEDIUM)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/base
    ydb/core/blobstorage
    ydb/core/erasure
)

SRCS(
    blobstorage_hullreplwritesst_ut.cpp
    ut/blobstorage_repl_eta_ut.cpp
    blobstorage_replrecoverymachine_ut.cpp
)

END()
