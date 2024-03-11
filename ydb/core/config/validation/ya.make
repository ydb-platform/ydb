LIBRARY()

SRCS(
    validators.h
)

PEERDIR(
    ydb/core/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)

