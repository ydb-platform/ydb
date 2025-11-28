LIBRARY()

SRCS(
    describer.cpp
)

PEERDIR(
    ydb/core/persqueue/events
    ydb/core/persqueue/public
)

GENERATE_ENUM_SERIALIZATION(describer.h)

END()

RECURSE_FOR_TESTS(
    ut
)
