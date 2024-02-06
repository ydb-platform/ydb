GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		binary.go
		native_endian_little.go
		varint.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		binary.go
		native_endian_little.go
		varint.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		binary.go
		native_endian_little.go
		varint.go
    )
ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		binary.go
		native_endian_little.go
		varint.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		binary.go
		native_endian_little.go
		varint.go
    )
ENDIF()
END()
