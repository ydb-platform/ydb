LIBRARY(isa-l_crc_yt_patch)

LICENSE(BSD-3-Clause)

VERSION(2.28)

NO_UTIL()

IF (ARCH_X86_64)
    # pclmul is required for fast crc computation
    CFLAGS(-mpclmul)
    SRCS(crc64_yt_norm_by8.asm)
ENDIF()

ADDINCL(
    FOR asm yt/yt/core/misc/isa_crc64/include  # for reg_sizes.asm
)

SRCS(
    crc64_yt_norm_refs.c

    checksum.cpp
)

END()

RECURSE_FOR_TESTS(
    unittests
)
