LIBRARY()

SRCS(
    path_normalizer.cpp
)

PEERDIR(
    ydb/core/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
