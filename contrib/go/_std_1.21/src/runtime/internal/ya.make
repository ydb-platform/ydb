RECURSE(
    atomic
    math
    sys
    # wasitest
)

IF (ARCH_X86_64)
    RECURSE(
        startlinetest
    )
ENDIF()

IF (OS_LINUX)
    RECURSE(
        syscall
    )
ENDIF()
