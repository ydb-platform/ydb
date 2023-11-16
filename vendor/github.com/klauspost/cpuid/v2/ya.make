GO_LIBRARY()

LICENSE(MIT)

SRCS(
    cpuid.go
    featureid_string.go
)

GO_TEST_SRCS(
    cpuid_test.go
    mockcpu_test.go
)

IF (ARCH_X86_64)
    SRCS(
        cpuid_amd64.s
        detect_x86.go
    )
ENDIF()

IF (ARCH_ARM64)
    SRCS(
        cpuid_arm64.s
        detect_arm64.go
    )
ENDIF()

IF (OS_LINUX AND ARCH_ARM64)
    SRCS(
        os_linux_arm64.go
        os_unsafe_linux_arm64.go
    )
ENDIF()

IF (OS_DARWIN)
    GO_TEST_SRCS(os_darwin_test.go)
ENDIF()

IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(os_darwin_arm64.go)
ENDIF()

IF (OS_WINDOWS AND ARCH_ARM64)
    SRCS(os_other_arm64.go)
ENDIF()

END()

RECURSE(gotest)
