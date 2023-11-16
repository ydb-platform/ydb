GO_LIBRARY()

SRCS(
    fd.go
    fd_mutex.go
    fd_poll_runtime.go
    fd_posix.go
    sockopt.go
    sockoptip.go
)

IF (OS_DARWIN)
    SRCS(
        errno_unix.go
        fcntl_libc.go
        fd_fsync_darwin.go
        fd_opendir_darwin.go
        fd_unix.go
        fd_writev_libc.go
        hook_unix.go
        iovec_unix.go
        sendfile_bsd.go
        sockopt_unix.go
        sys_cloexec.go
        writev.go
    )
ENDIF()

IF (OS_LINUX)
    SRCS(
        copy_file_range_linux.go
        errno_unix.go
        fcntl_syscall.go
        fd_fsync_posix.go
        fd_unix.go
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
ENDIF()

END()
