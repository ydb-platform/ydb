UNITTEST_FOR(ydb/library/actors/async)

PEERDIR(
    ydb/library/actors/testlib
)

SRCS(
    async_ut.cpp
    callback_coroutine_ut.cpp
    sleep_ut.cpp
    task_group_ut.cpp
)

END()
