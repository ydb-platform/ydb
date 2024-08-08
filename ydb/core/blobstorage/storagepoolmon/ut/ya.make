UNITTEST()

FORK_SUBTESTS()

SPLIT_FACTOR(20)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/blobstorage/storagepoolmon
    ydb/core/testlib/default
    ydb/core/testlib/actors
    ydb/core/testlib/basics
)

SRCS(
    storagepoolmon_ut.cpp
)

END()
