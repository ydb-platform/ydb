GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		atomic_arm64.go
		atomic_arm64.s
		doc.go
		stubs.go
		types.go
		types_64bit.go
		unaligned.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		atomic_amd64.go
		atomic_amd64.s
		doc.go
		stubs.go
		types.go
		types_64bit.go
		unaligned.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		atomic_arm64.go
		atomic_arm64.s
		doc.go
		stubs.go
		types.go
		types_64bit.go
		unaligned.go
    )
ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		atomic_amd64.go
		atomic_amd64.s
		doc.go
		stubs.go
		types.go
		types_64bit.go
		unaligned.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		atomic_amd64.go
		atomic_amd64.s
		doc.go
		stubs.go
		types.go
		types_64bit.go
		unaligned.go
    )
ENDIF()
END()
