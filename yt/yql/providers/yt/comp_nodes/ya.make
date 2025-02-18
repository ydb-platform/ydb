LIBRARY()

END()

RECURSE(
    llvm14
    llvm16
    no_llvm
)

RECURSE_FOR_TESTS(
    ut
)
