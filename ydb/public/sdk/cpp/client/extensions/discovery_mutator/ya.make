LIBRARY()

SRCS(
    discovery_mutator.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/client/ydb_extension
)

END()

RECURSE_FOR_TESTS(
    ut
)
