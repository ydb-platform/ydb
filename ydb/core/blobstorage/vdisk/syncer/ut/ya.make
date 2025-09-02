UNITTEST_FOR(ydb/core/blobstorage/vdisk/syncer)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    ydb/apps/version
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/base
    ydb/core/blobstorage
    ydb/core/testlib/default
)

SRCS(
    blobstorage_syncer_broker_ut.cpp
    blobstorage_syncer_data_ut.cpp
    blobstorage_syncer_localwriter_ut.cpp
    blobstorage_syncquorum_ut.cpp
)

END()
