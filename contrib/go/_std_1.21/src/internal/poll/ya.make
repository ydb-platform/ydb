GO_LIBRARY()

SRCS(
    fd.go
    fd_mutex.go
    fd_poll_runtime.go
    fd_posix.go
    sockopt.go
    sockoptip.go
)

GO_TEST_SRCS(
    export_posix_test.go
    export_test.go
)

GO_XTEST_SRCS(
    error_test.go
    fd_mutex_test.go
    fd_posix_test.go
    read_test.go
    writev_test.go
)

IF (OS_LINUX)
    SRCS(
        copy_file_range_linux.go
        errno_unix.go
        fd_fsync_posix.go
        fd_unix.go
        fd_unixjs.go
        fd_writev_unix.go
        hook_cloexec.go
        hook_unix.go
        iovec_unix.go
        sendfile_linux.go
        sock_cloexec.go
        sockopt_linux.go
        sockopt_unix.go
        splice_linux.go
        writev.go
    )

    GO_TEST_SRCS(export_linux_test.go)

    GO_XTEST_SRCS(
        error_linux_test.go
        splice_linux_test.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        errno_unix.go
        fd_fsync_darwin.go
        fd_opendir_darwin.go
        fd_unix.go
        fd_unixjs.go
        fd_writev_libc.go
        hook_unix.go
        iovec_unix.go
        sendfile_bsd.go
        sockopt_unix.go
        sys_cloexec.go
        writev.go
    )

    GO_XTEST_SRCS(error_stub_test.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        errno_windows.go
        fd_fsync_windows.go
        fd_windows.go
        hook_windows.go
        sendfile_windows.go
        sockopt_windows.go
    )

    GO_TEST_SRCS(export_windows_test.go)

    GO_XTEST_SRCS(
        error_stub_test.go
        fd_windows_test.go
    )
ENDIF()

END()

RECURSE(
)
