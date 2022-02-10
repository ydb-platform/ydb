UNITTEST_FOR(ydb/core/blobstorage/backpressure)

OWNER(g:kikimr)

FORK_SUBTESTS()

IF (WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion 
    ydb/core/base
    ydb/core/blobstorage
)

SRCS(
    queue_backpressure_client_ut.cpp
    queue_backpressure_server_ut.cpp
)

END()
