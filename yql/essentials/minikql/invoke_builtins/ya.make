LIBRARY()

SRCS(
    mkql_builtins.h  # gn: slot minikql_codegen
)

PEERDIR()

END()

RECURSE(
    llvm16
    no_llvm
)

RECURSE_FOR_TESTS(
    llvm16/ut
)
