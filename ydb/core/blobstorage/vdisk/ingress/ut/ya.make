UNITTEST_FOR(ydb/core/blobstorage/vdisk/ingress)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/core/blobstorage/groupinfo
    ydb/core/erasure
)

SRCS(
    blobstorage_ingress_matrix_ut.cpp
    blobstorage_ingress_ut.cpp
)

END()
