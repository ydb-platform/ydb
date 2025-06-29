UNITTEST_FOR(ydb/library/actors/async)

PEERDIR(
    ydb/library/actors/testlib
)

SRCS(
    async_ut.cpp
    callback_coroutine_ut.cpp
    cancellation_ut.cpp
    continuation_ut.cpp
    sleep_ut.cpp
    task_group_ut.cpp
    timeout_ut.cpp
)

END()
