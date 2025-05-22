UNITTEST()

FORK_SUBTESTS()

SPLIT_FACTOR(20)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
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
