LIBRARY()

SRCS()

PEERDIR(
    ydb/library/yql/dq/comp_nodes/llvm16
)

END()

RECURSE(
    llvm16
    no_llvm
)

RECURSE_FOR_TESTS(
    ut
)
