GO_LIBRARY()

SRCS(
    exec.go
)

GO_TEST_SRCS(
    bench_test.go
    env_test.go
    internal_test.go
    lp_test.go
)

GO_XTEST_SRCS(
    dot_test.go
    example_test.go
    exec_test.go
)

IF (OS_LINUX)
    SRCS(
        exec_unix.go
        lp_unix.go
    )

    GO_TEST_SRCS(lp_unix_test.go)

    GO_XTEST_SRCS(
        exec_linux_test.go
        exec_posix_test.go
        exec_unix_test.go
        lp_linux_test.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        exec_unix.go
        lp_unix.go
    )

    GO_TEST_SRCS(lp_unix_test.go)

    GO_XTEST_SRCS(
        exec_posix_test.go
        exec_unix_test.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        exec_windows.go
        lp_windows.go
    )

    GO_XTEST_SRCS(
        exec_windows_test.go
        lp_windows_test.go
    )
ENDIF()

END()

RECURSE(
    internal
)
