LIBRARY()

END()

RECURSE(
    llvm14
    no_llvm
)

RECURSE_FOR_TESTS(
    ut
)
