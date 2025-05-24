LIBRARY()

SRCS(
)

PEERDIR(
    ydb/core/tx/conveyor_composite/service
    ydb/core/tx/conveor_composite/usage
)

END()

RECURSE_FOR_TESTS(
    ut
)
