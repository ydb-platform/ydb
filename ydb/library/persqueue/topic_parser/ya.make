LIBRARY()

PEERDIR(
    ydb/core/base
    ydb/library/persqueue/topic_parser_public
    ydb/public/api/protos
)

SRCS(
    topic_parser.h
    topic_parser.cpp
    counters.h
    counters.cpp
    type_definitions.h
)

END()

RECURSE_FOR_TESTS(
    ut
)
