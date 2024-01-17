GO_LIBRARY()

SRCS(
    goarch.go
)

IF (ARCH_X86_64)
    SRCS(
        goarch_amd64.go
        zgoarch_amd64.go
    )
ENDIF()

IF (ARCH_ARM64)
    SRCS(
        goarch_arm64.go
        zgoarch_arm64.go
    )
ENDIF()

END()
