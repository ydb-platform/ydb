GO_LIBRARY()
IF (FALSE)
    MESSAGE(FATAL this shall never happen)

ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		func_amd64.go
		func_amd64.s
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		func_amd64.go
		func_amd64.s
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		func_amd64.go
		func_amd64.s
    )
ENDIF()
END()
