UNITTEST_FOR(ydb/library/actors/core)

FORK_SUBTESTS()
IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    SPLIT_FACTOR(20)
    REQUIREMENTS(
        ram:32
    )
ELSE()
    SIZE(MEDIUM)
ENDIF()


PEERDIR(
    ydb/library/actors/interconnect
    ydb/library/actors/testlib
)

SRCS(
    actor_basic_ut.cpp
    actor_coroutine_ut.cpp
    actor_exception_ut.cpp
    actor_shared_threads.cpp
    benchmark_ut.cpp
    actor_ut.cpp
    actorsystem_ut.cpp
    performance_ut.cpp
    process_stats_ut.cpp
    ask_ut.cpp
    event_pb_payload_ut.cpp
    event_pb_ut.cpp
    executor_pool_basic_ut.cpp
    log_ut.cpp
    mon_ut.cpp
    scheduler_actor_ut.cpp
    mailbox_lockfree_ut.cpp
)

END()
