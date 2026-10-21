LIBRARY()

NO_COMPILER_WARNINGS()

ADDINCL(
    GLOBAL ydb/tests/hash_test/xxHash
)

SRCS(
    xxhash.c
)

# xxh_x86dispatch.c hard #error's on any non-x86 target (it only implements
# runtime dispatch for x86/x86_64); keep it out of ARM64 builds.
IF (NOT ARCH_ARM64)
    SRCS(
        xxh_x86dispatch.c
    )
ENDIF()

END()
