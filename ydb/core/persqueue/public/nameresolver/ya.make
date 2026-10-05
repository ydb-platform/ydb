LIBRARY()

SRCS(
    nameresolver.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/public/sdk/cpp/src/library/persqueue/topic_parser_public
)

END()

RECURSE_FOR_TESTS(
    ut
)
