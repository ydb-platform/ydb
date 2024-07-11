UNITTEST_FOR(ydb/core/memory_controller)

SIZE(SMALL)

TIMEOUT(60)

PEERDIR(
    ydb/core/memory_controller
)

SRCS(
    memory_controller_ut.cpp
)

END()
