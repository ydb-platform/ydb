LIBRARY()

SRCS(
    abi.cpp
    abi.h
    async.cpp
    async.h
    callback_coroutine.cpp
    callback_coroutine.h
    continuation.cpp
    continuation.h
    result.cpp
    result.h
    sleep.cpp
    sleep.h
    symmetric_proxy.h
    task_group.cpp
    task_group.h
    wait_for_event.cpp
    wait_for_event.h
)

PEERDIR(
    ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(
    benchmark
    ut
)
