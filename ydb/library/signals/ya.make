LIBRARY()

SRCS(
    agent.cpp
    client.cpp
    owner.cpp
    private.cpp
    object_counter.cpp
    histogram.cpp
    signal_utils.cpp
    states.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    library/cpp/threading/future
    ydb/core/protos
    ydb/core/base
)

END()
