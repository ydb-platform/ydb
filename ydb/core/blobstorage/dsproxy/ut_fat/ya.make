UNITTEST()

FORK_SUBTESTS()

SPLIT_FACTOR(30)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:10)
ENDIF()

IF (WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
    TIMEOUT(1800)
ELSE()
    SIZE(MEDIUM)
    TIMEOUT(600)
ENDIF()

PEERDIR(
    ydb/library/actors/protos
    ydb/library/actors/util
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

END()
