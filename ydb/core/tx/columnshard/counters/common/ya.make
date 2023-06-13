LIBRARY()

SRCS(
    agent.cpp
    client.cpp
    owner.cpp
    private.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    ydb/core/base
)

END()
