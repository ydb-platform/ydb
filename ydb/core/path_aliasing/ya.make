LIBRARY()

SRCS(
    path_normalizer.cpp
)

PEERDIR(
    contrib/libs/re2
    ydb/core/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
