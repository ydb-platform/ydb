LIBRARY()

SRCS(
    describer.cpp
)

PEERDIR(
    library/cpp/containers/absl
    ydb/core/persqueue/events
    ydb/core/persqueue/public/nameresolver
#    ydb/core/persqueue/public
)

GENERATE_ENUM_SERIALIZATION(describer.h)

END()

RECURSE_FOR_TESTS(
    ut
)
