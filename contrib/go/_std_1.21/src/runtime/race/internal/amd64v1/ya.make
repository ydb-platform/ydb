GO_LIBRARY()

IF (ARCH_X86_64)
    SRCS(
        doc.go
    )
ENDIF()

IF (RACE)
    IF (OS_DARWIN AND ARCH_X86_64)
        SRCS(
            race_darwin.syso
        )
    ENDIF()

    IF (OS_LINUX AND ARCH_X86_64)
        SRCS(
            race_linux.syso
        )
    ENDIF()

    IF (OS_WINDOWS AND ARCH_X86_64)
        SRCS(
            race_windows.syso
        )
    ENDIF()
ENDIF()

END()
