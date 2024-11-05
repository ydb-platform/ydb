LIBRARY()

SRCS(
    validators.h
    validators.cpp
)

PEERDIR(
    ydb/core/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)

