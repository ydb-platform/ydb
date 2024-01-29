GO_LIBRARY()
IF (FALSE)
    MESSAGE(FATAL this shall never happen)

ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		bidi.go
		bracket.go
		core.go
		prop.go
		tables15.0.0.go
		trieval.go
    )
ELSEIF (OS_LINUX AND ARCH_ARM64)
    SRCS(
		bidi.go
		bracket.go
		core.go
		prop.go
		tables15.0.0.go
		trieval.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		bidi.go
		bracket.go
		core.go
		prop.go
		tables15.0.0.go
		trieval.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		bidi.go
		bracket.go
		core.go
		prop.go
		tables15.0.0.go
		trieval.go
    )
ELSEIF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		bidi.go
		bracket.go
		core.go
		prop.go
		tables15.0.0.go
		trieval.go
    )
ELSEIF (OS_DARWIN AND ARCH_AARCH64)
    SRCS(
		bidi.go
		bracket.go
		core.go
		prop.go
		tables15.0.0.go
		trieval.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		bidi.go
		bracket.go
		core.go
		prop.go
		tables15.0.0.go
		trieval.go
    )
ELSEIF (OS_WINDOWS AND ARCH_ARM64)
    SRCS(
		bidi.go
		bracket.go
		core.go
		prop.go
		tables15.0.0.go
		trieval.go
    )
ELSEIF (OS_WINDOWS AND ARCH_AARCH64)
    SRCS(
		bidi.go
		bracket.go
		core.go
		prop.go
		tables15.0.0.go
		trieval.go
    )
ENDIF()
END()
