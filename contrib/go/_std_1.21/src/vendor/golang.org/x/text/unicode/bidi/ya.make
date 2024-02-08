GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64)
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
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		bidi.go
		bracket.go
		core.go
		prop.go
		tables15.0.0.go
		trieval.go
    )
ELSEIF (OS_LINUX AND ARCH_X86_64)
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
ENDIF()
END()
