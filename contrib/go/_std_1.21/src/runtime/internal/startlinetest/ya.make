GO_LIBRARY()

IF (ARCH_X86_64)
    SRCS(
        func_amd64.go
        func_amd64.s
    )
ENDIF()

END()
