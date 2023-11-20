GO_LIBRARY()

LICENSE(BSD-3-Clause)

IF (OS_LINUX AND ARCH_X86_64)
    SRCS(
        capi_linux_amd64.go
        types_linux_amd64.go
    )
ENDIF()

IF (OS_LINUX AND ARCH_ARM64)
    SRCS(
        capi_linux_arm64.go
        types_linux_arm64.go
    )
ENDIF()

IF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
        capi_darwin_amd64.go
        types_darwin_amd64.go
    )
ENDIF()

IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
        capi_darwin_arm64.go
        types_darwin_arm64.go
    )
ENDIF()

IF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
        capi_windows_amd64.go
        types_windows_amd64.go
    )
ENDIF()

IF (OS_WINDOWS AND ARCH_ARM64)
    SRCS(
        capi_windows_arm64.go
        types_windows_arm64.go
    )
ENDIF()

END()
