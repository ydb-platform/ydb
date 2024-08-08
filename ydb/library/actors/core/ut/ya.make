UNITTEST_FOR(ydb/library/actors/core)

FORK_SUBTESTS()
IF (SANITIZER_TYPE)
    SIZE(LARGE)
    TIMEOUT(1200)
    TAG(ya:fat)
    SPLIT_FACTOR(20)
    REQUIREMENTS(
        ram:32
    )
ELSE()
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
    TIMEOUT(600)
    REQUIREMENTS(
        ram:16
    )
ENDIF()


PEERDIR(
    ydb/library/actors/interconnect
    ydb/library/actors/testlib
)

SRCS(
    actor_coroutine_ut.cpp
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
)

END()
