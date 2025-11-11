IF (ARCH_X86_64 AND OS_LINUX)

LIBRARY()

PEERDIR(library/cpp/digest/crc32c)

END()

RECURSE_FOR_TESTS(
    ut
    exec
)

ENDIF()