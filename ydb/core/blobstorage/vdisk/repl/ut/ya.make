UNITTEST_FOR(ydb/core/blobstorage/vdisk/repl)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/base
    ydb/core/blobstorage
    ydb/core/erasure
    ydb/core/blobstorage/vdisk/syncer
    ydb/core/testlib/default
    ydb/core/testlib/actors
)

SRCS(
    blobstorage_hullreplwritesst_ut.cpp
    blobstorage_replrecoverymachine_ut.cpp
    blobstorage_repl_ut.cpp
)

END()
