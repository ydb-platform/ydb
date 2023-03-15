LIBRARY()

PEERDIR(
    contrib/libs/hyperscan
    contrib/libs/hyperscan/runtime_core2
    contrib/libs/hyperscan/runtime_corei7
    contrib/libs/hyperscan/runtime_avx2
    contrib/libs/hyperscan/runtime_avx512
)

SRCS(
    hyperscan.cpp
)

END()

RECURSE_FOR_TESTS(ut)
