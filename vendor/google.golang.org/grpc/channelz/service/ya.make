GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(service.go)

GO_TEST_SRCS(service_test.go)

IF (OS_LINUX)
    SRCS(func_linux.go)
ENDIF()

IF (OS_LINUX AND ARCH_X86_64)
    GO_TEST_SRCS(
        service_sktopt_test.go
        util_sktopt_amd64_test.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(func_nonlinux.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(func_nonlinux.go)
ENDIF()

END()

RECURSE(gotest)
