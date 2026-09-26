LIBRARY()

SRCS(
    quoter.cpp
)

PEERDIR(
    library/cpp/containers/absl
    ydb/core/persqueue/events
    ydb/core/util
    ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(
    ut
)
