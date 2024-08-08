UNITTEST_FOR(ydb/core/blobstorage/vdisk/synclog)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    library/cpp/testing/unittest
    ydb/core/base
    ydb/core/blobstorage
)

SRCS(
    blobstorage_synclogdata_ut.cpp
    blobstorage_synclogdsk_ut.cpp
    blobstorage_synclogkeeper_ut.cpp
    blobstorage_synclogmem_ut.cpp
    blobstorage_synclogmsgimpl_ut.cpp
    blobstorage_synclogmsgwriter_ut.cpp
    codecs_ut.cpp
)

END()
