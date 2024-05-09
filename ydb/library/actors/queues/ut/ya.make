UNITTEST_FOR(ydb/library/actors/queues)

IF (WITH_VALGRIND)
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    mpmc_ring_queue_ut_single_thread.cpp
    mpmc_ring_queue_ut_multi_threads.cpp
)

END()
