LIBRARY()

SRCS(
    optimizer.cpp
    intervals_optimizer.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/protos
    ydb/core/formats/arrow
)

END()

RECURSE_FOR_TESTS(
    ut
)
