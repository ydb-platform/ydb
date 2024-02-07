GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		goarch.go
		goarch_arm64.go
		zgoarch_arm64.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		goarch.go
		goarch_amd64.go
		zgoarch_amd64.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		goarch.go
		goarch_arm64.go
		zgoarch_arm64.go
    )
ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		goarch.go
		goarch_amd64.go
		zgoarch_amd64.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		goarch.go
		goarch_amd64.go
		zgoarch_amd64.go
    )
ENDIF()
END()
