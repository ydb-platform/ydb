GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		encodedword.go
		grammar.go
		mediatype.go
		type.go
		type_unix.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		encodedword.go
		grammar.go
		mediatype.go
		type.go
		type_unix.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		encodedword.go
		grammar.go
		mediatype.go
		type.go
		type_unix.go
    )
ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		encodedword.go
		grammar.go
		mediatype.go
		type.go
		type_unix.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		encodedword.go
		grammar.go
		mediatype.go
		type.go
		type_windows.go
    )
ENDIF()
END()


RECURSE(
	multipart
	quotedprintable
)
