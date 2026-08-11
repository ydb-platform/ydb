LIBRARY()

SRCS(
    histogram.cpp
    logging.cpp
)

PEERDIR(
    library/cpp/lwtrace
    library/cpp/unified_agent_client
    util
    ydb/core/protos/nbs
    ydb/library/services
)

END()
