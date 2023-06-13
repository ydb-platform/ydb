LIBRARY()

IF (ARCH_X86_64 OR ARCH_I386)
    PEERDIR(
        library/cpp/threading/poor_man_openmp
        library/cpp/digest/argonish/internal/proxies/avx2
        library/cpp/digest/argonish/internal/proxies/ref
        library/cpp/digest/argonish/internal/proxies/sse2
        library/cpp/digest/argonish/internal/proxies/sse41
        library/cpp/digest/argonish/internal/proxies/ssse3
    )
ELSE()
    PEERDIR(
        library/cpp/threading/poor_man_openmp
        library/cpp/digest/argonish/internal/proxies/ref
    )
ENDIF()

SRCS(
    factory/factory.cpp
)

END()

RECURSE(
    benchmark
    ut
    ut_fat
)
