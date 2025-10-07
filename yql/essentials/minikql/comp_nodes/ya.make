LIBRARY()

SRCS(
)

PEERDIR(
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    llvm16
    llvm20
    no_llvm
)

RECURSE_FOR_TESTS(
    llvm16/ut
    llvm20/ut
    benchmark
)
