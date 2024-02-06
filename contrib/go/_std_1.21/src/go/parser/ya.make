GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		interface.go
		parser.go
		resolver.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		interface.go
		parser.go
		resolver.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		interface.go
		parser.go
		resolver.go
    )
ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		interface.go
		parser.go
		resolver.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		interface.go
		parser.go
		resolver.go
    )
ENDIF()
END()
