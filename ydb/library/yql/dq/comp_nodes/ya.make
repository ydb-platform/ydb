LIBRARY()

SRCS()
CFLAGS(-Wmissing-field-initializers)

# TODO: Migrate all dependents to explicit llvm16/no_llvm and remove this peerdir
PEERDIR(
    ydb/library/yql/dq/comp_nodes/llvm16
)

END()

RECURSE(
    llvm16
    no_llvm
    hash_join_utils
)

RECURSE_FOR_TESTS(
    ut
)
