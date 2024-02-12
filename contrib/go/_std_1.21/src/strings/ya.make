GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		builder.go
		clone.go
		compare.go
		reader.go
		replace.go
		search.go
		strings.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		builder.go
		clone.go
		compare.go
		reader.go
		replace.go
		search.go
		strings.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		builder.go
		clone.go
		compare.go
		reader.go
		replace.go
		search.go
		strings.go
    )
ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		builder.go
		clone.go
		compare.go
		reader.go
		replace.go
		search.go
		strings.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		builder.go
		clone.go
		compare.go
		reader.go
		replace.go
		search.go
		strings.go
    )
ENDIF()
END()
