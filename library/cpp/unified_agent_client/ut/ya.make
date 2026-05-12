GTEST()

SRCS(
    async_joiner_ut.cpp
    logger_ut.cpp
)

PEERDIR(
    library/cpp/unified_agent_client
    library/cpp/testing/gtest
)

END()
