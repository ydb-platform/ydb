LIBRARY()

SRCS()

PEERDIR(
    ydb/library/yql/dq/comp_nodes/llvm16
)

END()

RECURSE(
    llvm16
)

RECURSE_FOR_TESTS(
    ut
)
