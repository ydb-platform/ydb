LIBRARY()

SRCS(
    collector.cpp
    metrics.cpp
    observation.cpp
)

PEERDIR(
    ydb/library/accessor
    library/cpp/time_provider
    contrib/libs/protobuf
    ydb/core/protos
)

END()

RECURSE_FOR_TESTS(ut)

RECURSE(reader transport)
