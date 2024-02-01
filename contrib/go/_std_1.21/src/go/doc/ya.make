GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		comment.go
		doc.go
		example.go
		exports.go
		filter.go
		reader.go
		synopsis.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		comment.go
		doc.go
		example.go
		exports.go
		filter.go
		reader.go
		synopsis.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		comment.go
		doc.go
		example.go
		exports.go
		filter.go
		reader.go
		synopsis.go
    )
ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		comment.go
		doc.go
		example.go
		exports.go
		filter.go
		reader.go
		synopsis.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		comment.go
		doc.go
		example.go
		exports.go
		filter.go
		reader.go
		synopsis.go
    )
ENDIF()
END()


RECURSE(
	comment
)
