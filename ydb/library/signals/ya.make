LIBRARY()

SRCS(
    agent.cpp
    client.cpp
    owner.cpp
    private.cpp
    object_counter.cpp
    histogram.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    ydb/core/protos
    ydb/core/base
)

END()
