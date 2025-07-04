UNITTEST_FOR(ydb/core/blobstorage/nodewarden)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/core/testlib/default
)

SRCS(
    blobstorage_node_warden_ut.cpp
    bind_queue_ut.cpp
    distconf_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
