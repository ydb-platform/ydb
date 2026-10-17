LIBRARY()

PEERDIR(library/cpp/digest/crc32c)

END()

IF (ARCH_X86_64 AND OS_LINUX)

RECURSE(
    layout/avx2
    layout/sse42
)

RECURSE_FOR_TESTS(
    ut
    exec
)

ENDIF()
