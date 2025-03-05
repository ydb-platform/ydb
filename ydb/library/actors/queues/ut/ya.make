UNITTEST_FOR(ydb/library/actors/queues)

IF (WITH_VALGRIND)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    mpmc_ring_queue_ut_single_thread.cpp
    mpmc_ring_queue_ut_multi_threads.cpp

    mpmc_ring_queue_v2_ut_single_thread.cpp
    mpmc_ring_queue_v2_ut_multi_threads.cpp

    mpmc_bitmap_buffer_ut.cpp
)

PEERDIR(
    ydb/library/actors/queues/observer
)


END()
