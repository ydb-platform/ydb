UNITTEST_FOR(ydb/core/blobstorage/nodewarden)

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:14)
ENDIF()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(1200)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/core/testlib/default
)

SRCS(
    blobstorage_node_warden_ut.cpp
    bind_queue_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
