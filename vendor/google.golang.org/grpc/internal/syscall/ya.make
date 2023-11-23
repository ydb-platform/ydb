GO_LIBRARY()

LICENSE(Apache-2.0)

IF (OS_LINUX)
    SRCS(syscall_linux.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(syscall_nonlinux.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(syscall_nonlinux.go)
ENDIF()

END()
