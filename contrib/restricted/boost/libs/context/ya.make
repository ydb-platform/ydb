LIBRARY()

LICENSE(BSL-1.0)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

OWNER(
    antoshkka
    g:cpp-committee
    g:cpp-contrib
)

INCLUDE(${ARCADIA_ROOT}/contrib/restricted/boost/boost_common.inc)

CFLAGS(
    -DBOOST_CONTEXT_SOURCE
)

# https://www.boost.org/doc/libs/1_74_0/libs/context/doc/html/context/stack/sanitizers.html 
IF (SANITIZER_TYPE == "address") 
    CFLAGS( 
        GLOBAL -DBOOST_USE_UCONTEXT 
        GLOBAL -DBOOST_USE_ASAN 
    ) 
    SRCS( 
        src/continuation.cpp 
        src/fiber.cpp 
    ) 
ENDIF() 
 
IF (OS_WINDOWS)
    IF (ARCH_X86_64 OR ARCH_I386)
        MASMFLAGS('-DBOOST_CONTEXT_EXPORT= /safeseh')
    ENDIF()
    SRCS(
        src/windows/stack_traits.cpp
    )
    IF (ARCH_X86_64 OR ARCH_I386)
        IF (ARCH_X86_64)
            SRCS(
                src/asm/jump_x86_64_ms_pe_masm.masm
                src/asm/make_x86_64_ms_pe_masm.masm
                src/asm/ontop_x86_64_ms_pe_masm.masm
            )
        ENDIF()
        IF (ARCH_I386)
            SRCS(
                src/asm/jump_i386_ms_pe_masm.masm
                src/asm/make_i386_ms_pe_masm.masm
                src/asm/ontop_i386_ms_pe_masm.masm
            )
        ENDIF()
    ENDIF()
    IF (ARCH_ARM)
        SRCS(
            src/asm/jump_arm_aapcs_pe_armasm.masm
            src/asm/make_arm_aapcs_pe_armasm.masm
            src/asm/ontop_arm_aapcs_pe_armasm.masm
        )
    ENDIF()
ELSEIF (OS_DARWIN OR OS_IOS)
    IF (ARCH_ARM7)
        SRCS(
            src/asm/jump_arm_aapcs_macho_gas.S
            src/asm/make_arm_aapcs_macho_gas.S
            src/asm/ontop_arm_aapcs_macho_gas.S
        )
    ELSEIF (ARCH_ARM64)
        SRCS(
            src/asm/jump_arm64_aapcs_macho_gas.S
            src/asm/make_arm64_aapcs_macho_gas.S
            src/asm/ontop_arm64_aapcs_macho_gas.S
        )
    ELSEIF (ARCH_I386)
        SRCS(
            src/asm/jump_i386_sysv_macho_gas.S
            src/asm/make_i386_sysv_macho_gas.S
            src/asm/ontop_i386_sysv_macho_gas.S
        )
    ELSE()
        SRCS(
            src/asm/jump_x86_64_sysv_macho_gas.S
            src/asm/make_x86_64_sysv_macho_gas.S
            src/asm/ontop_x86_64_sysv_macho_gas.S
        )
    ENDIF()
    SRCS(
        src/posix/stack_traits.cpp
    )
ELSE()
    IF (ARCH_ARM7)
        SRCS(
            src/asm/jump_arm_aapcs_elf_gas.S
            src/asm/make_arm_aapcs_elf_gas.S
            src/asm/ontop_arm_aapcs_elf_gas.S
        )
    ELSEIF (ARCH_ARM64)
        SRCS(
            src/asm/jump_arm64_aapcs_elf_gas.S
            src/asm/make_arm64_aapcs_elf_gas.S
            src/asm/ontop_arm64_aapcs_elf_gas.S
        )
    ELSEIF (ARCH_I386)
        SRCS(
            src/asm/jump_i386_sysv_elf_gas.S
            src/asm/make_i386_sysv_elf_gas.S
            src/asm/ontop_i386_sysv_elf_gas.S
        )
    ELSE()
        SRCS(
            src/asm/jump_x86_64_sysv_elf_gas.S
            src/asm/make_x86_64_sysv_elf_gas.S
            src/asm/ontop_x86_64_sysv_elf_gas.S
        )
    ENDIF()
    SRCS(
        src/posix/stack_traits.cpp
    )
ENDIF()

END()
