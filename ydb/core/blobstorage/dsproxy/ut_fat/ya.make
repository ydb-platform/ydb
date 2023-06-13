UNITTEST()

FORK_SUBTESTS()

SPLIT_FACTOR(30)

IF (WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
    TIMEOUT(1800)
ELSE()
    SIZE(MEDIUM)
    TIMEOUT(600)
ENDIF()

PEERDIR(
    library/cpp/actors/protos
    library/cpp/actors/util
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/core/blobstorage/dsproxy
    ydb/core/blobstorage/groupinfo
    ydb/core/blobstorage/pdisk
    ydb/core/blobstorage/vdisk
    ydb/core/blobstorage/vdisk/common
    ydb/core/testlib/default
)

SRCS(
    dsproxy_ut.cpp
)

REQUIREMENTS(ram:10)

END()
