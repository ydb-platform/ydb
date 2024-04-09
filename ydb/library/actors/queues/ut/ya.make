UNITTEST_FOR(ydb/library/actors/queues)

IF (WITH_VALGRIND)
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    mpmc_ring_queue_ut.cpp
)

END()
