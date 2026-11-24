LIBRARY()

SRCS(
    metrics.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/library/persqueue/topic_parser
)

END()

RECURSE_FOR_TESTS(
    ut
)
