LIBRARY()

PEERDIR(library/cpp/digest/crc32c)

END()

IF (ARCH_X86_64 AND OS_LINUX)

RECURSE(
    packing/avx2
    packing/sse42
)

RECURSE_FOR_TESTS(
    ut
    exec
)

ENDIF()
