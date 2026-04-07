LIBRARY()

SRCS(
    histogram.cpp
    logging.cpp
)

PEERDIR(
    library/cpp/lwtrace
    util
    ydb/core/protos/nbs
    ydb/library/services
)

END()

