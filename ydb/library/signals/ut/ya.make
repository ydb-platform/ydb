GTEST()

PEERDIR(
    ydb/library/signals
)

SRCS(
    agent_ut.cpp
)

GENERATE_ENUM_SERIALIZATION(test_states.h)

END()
