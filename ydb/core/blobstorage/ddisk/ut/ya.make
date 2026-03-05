UNITTEST_FOR(ydb/core/blobstorage/ddisk)

FORK_SUBTESTS()

IF (WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/core/blobstorage/ddisk
    ydb/core/util/actorsys_test
)

SRCS(
    ddisk_actor_ut.cpp
    segment_manager_ut.cpp
)

END()
