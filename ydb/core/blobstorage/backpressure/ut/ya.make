UNITTEST_FOR(ydb/core/blobstorage/backpressure)

FORK_SUBTESTS()

IF (WITH_VALGRIND)
    TIMEOUT(1800)
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
    ydb/core/base
    ydb/core/blobstorage/dsproxy/mock
)

SRCS(
    queue_backpressure_client_ut.cpp
    queue_backpressure_server_ut.cpp
)

END()
