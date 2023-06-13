LIBRARY()

LICENSE(
    GPL-1.0-or-later AND
    GPL-2.0-only AND
    GPL-3.0-or-later AND
    LGPL-2.0-or-later AND
    LGPL-3.0-only
)

VERSION(2016-11-16)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

ORIGINAL_SOURCE(https://www.agner.org/optimize/)

NO_PLATFORM()

SET(_YASM_PREDEFINED_FLAGS_VALUE "")

IF (ARCH_X86_64)
    IF (OS_DARWIN)
        PEERDIR(
            contrib/libs/asmglibc
        )
    ENDIF()
    IF (NOT OS_DARWIN)
        SRCS(
            sfmt64.asm
            mother64.asm
            mersenne64.asm
        )
    ENDIF()
    SRCS(
        debugbreak64.asm
        cachesize64.asm
        divfixedi64.asm
        rdtsc64.asm
        strcat64.asm
        unalignedisfaster64.asm
        strcpy64.asm
        substring64.asm
        strlen64.asm
        cputype64.asm
        memcmp64.asm
        memmove64.asm
        stricmp64.asm
        divfixedv64.asm
        physseed64.asm
        cpuid64.asm
        round64.asm
        memcpy64.asm
        popcount64.asm
        dispatchpatch64.asm
        #instrset64.asm
        procname64.asm
        memset64.asm
        #disabled because of protection violation
        #strcountutf864.asm
        #strcountset64.asm
        #strtouplow64.asm
        #strcmp64.asm
        #strspn64.asm
        #strstr64.asm
    )
ENDIF()

IF (ARCH_I386)
    SRCS(
        debugbreak32.asm
        cachesize32.asm
        divfixedi32.asm
        rdtsc32.asm
        strcat32.asm
        unalignedisfaster32.asm
        strcpy32.asm
        substring32.asm
        strlen32.asm
        cputype32.asm
        memcmp32.asm
        memmove32.asm
        sfmt32.asm
        stricmp32.asm
        divfixedv32.asm
        physseed32.asm
        cpuid32.asm
        mother32.asm
        round32.asm
        mersenne32.asm
        memcpy32.asm
        popcount32.asm
        dispatchpatch32.asm
        #instrset32.asm
        procname32.asm
        memset32.asm
        #disabled because of protection violation
        #strcountutf832.asm
        #strcountset32.asm
        #strtouplow32.asm
        #strcmp32.asm
        #strspn32.asm
        #strstr32.asm
    )
ENDIF()

SRCS(
    dummy.c
)

END()
