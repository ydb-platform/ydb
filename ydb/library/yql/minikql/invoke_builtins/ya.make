LIBRARY()

SRCS(
)

PEERDIR(
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    llvm14
    no_llvm
)

RECURSE_FOR_TESTS(
    llvm14/ut
)
