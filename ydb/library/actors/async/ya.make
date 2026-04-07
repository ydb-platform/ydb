LIBRARY()

SRCS(
    abi.cpp
    abi.h
    async.cpp
    async.h
    callback_coroutine.cpp
    callback_coroutine.h
    cancellation.cpp
    cancellation.h
    continuation.cpp
    continuation.h
    decorator.cpp
    decorator.h
    event.cpp
    event.h
    low_priority.cpp
    low_priority.h
    result.cpp
    result.h
    sleep.cpp
    sleep.h
    task_group.cpp
    task_group.h
    timeout.cpp
    timeout.h
    wait_for_event.cpp
    wait_for_event.h
    yield.cpp
    yield.h
)

PEERDIR(
    ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(
    benchmark
    ut
)
