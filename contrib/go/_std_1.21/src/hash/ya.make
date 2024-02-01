GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		hash.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		hash.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		hash.go
    )
ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		hash.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		hash.go
    )
ENDIF()
END()


RECURSE(
	adler32
	crc32
	crc64
	fnv
	maphash
)
