GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		compile.go
		doc.go
		op_string.go
		parse.go
		perl_groups.go
		prog.go
		regexp.go
		simplify.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		compile.go
		doc.go
		op_string.go
		parse.go
		perl_groups.go
		prog.go
		regexp.go
		simplify.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		compile.go
		doc.go
		op_string.go
		parse.go
		perl_groups.go
		prog.go
		regexp.go
		simplify.go
    )
ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		compile.go
		doc.go
		op_string.go
		parse.go
		perl_groups.go
		prog.go
		regexp.go
		simplify.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		compile.go
		doc.go
		op_string.go
		parse.go
		perl_groups.go
		prog.go
		regexp.go
		simplify.go
    )
ENDIF()
END()
