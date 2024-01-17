GO_LIBRARY()

SRCS(
    dir.go
    endian_little.go
    env.go
    error.go
    error_errno.go
    error_posix.go
    exec.go
    exec_posix.go
    executable.go
    file.go
    file_posix.go
    getwd.go
    path.go
    proc.go
    rawconn.go
    stat.go
    str.go
    sys.go
    tempfile.go
    types.go
)

GO_TEST_SRCS(export_test.go)

GO_XTEST_SRCS(
    env_test.go
    error_test.go
    example_test.go
    executable_test.go
    os_test.go
    path_test.go
    pipe_test.go
    rawconn_test.go
    read_test.go
    removeall_test.go
    rlimit_test.go
    stat_test.go
    tempfile_test.go
)

IF (OS_LINUX)
    SRCS(
        dir_unix.go
        dirent_linux.go
        exec_unix.go
        executable_procfs.go
        file_open_unix.go
        file_unix.go
        path_unix.go
        pipe2_unix.go
        readfrom_linux.go
        removeall_at.go
        rlimit.go
        rlimit_stub.go
        stat_linux.go
        stat_unix.go
        sticky_notbsd.go
        sys_linux.go
        sys_unix.go
        types_unix.go
        wait_waitid.go
    )

    GO_TEST_SRCS(
        export_linux_test.go
        export_unix_test.go
    )

    GO_XTEST_SRCS(
        env_unix_test.go
        error_unix_test.go
        exec_unix_test.go
        fifo_test.go
        os_unix_test.go
        readfrom_linux_test.go
        timeout_test.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        dir_darwin.go
        exec_unix.go
        executable_darwin.go
        file_open_unix.go
        file_unix.go
        path_unix.go
        pipe_unix.go
        readfrom_stub.go
        removeall_at.go
        rlimit.go
        rlimit_darwin.go
        stat_darwin.go
        stat_unix.go
        sticky_bsd.go
        sys_bsd.go
        sys_unix.go
        types_unix.go
        wait_unimp.go
    )

    GO_TEST_SRCS(export_unix_test.go)

    GO_XTEST_SRCS(
        env_unix_test.go
        error_unix_test.go
        exec_unix_test.go
        fifo_test.go
        os_unix_test.go
        timeout_test.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        dir_windows.go
        exec_windows.go
        executable_windows.go
        file_windows.go
        path_windows.go
        readfrom_stub.go
        removeall_noat.go
        stat_windows.go
        sticky_notbsd.go
        sys_windows.go
        types_windows.go
    )

    GO_TEST_SRCS(export_windows_test.go)

    GO_XTEST_SRCS(
        error_windows_test.go
        os_windows_test.go
        path_windows_test.go
    )
ENDIF()

END()

RECURSE(
    exec
    signal
    user
)
