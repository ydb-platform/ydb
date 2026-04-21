UNITTEST_FOR(ydb/core/actor_tracing)

SIZE(MEDIUM)

SRCS(
    tree_broadcast_ut.cpp
)

PEERDIR(
    ydb/core/actor_tracing
    ydb/library/actors/testlib
    ydb/library/actors/core
)

END()
