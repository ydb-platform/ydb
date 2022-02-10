UNITTEST_FOR(library/cpp/actors/core)

OWNER(
    alexvru
    g:kikimr
)

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
    TIMEOUT(600)
    REQUIREMENTS(
        ram:16
    )
ENDIF()


PEERDIR(
    library/cpp/actors/interconnect
    library/cpp/actors/testlib
)

SRCS(
    actor_coroutine_ut.cpp
    actor_ut.cpp
    actorsystem_ut.cpp
    ask_ut.cpp 
    balancer_ut.cpp
    event_pb_payload_ut.cpp
    event_pb_ut.cpp
    executor_pool_basic_ut.cpp
    executor_pool_united_ut.cpp
    log_ut.cpp
    memory_tracker_ut.cpp
    scheduler_actor_ut.cpp
)

END()
