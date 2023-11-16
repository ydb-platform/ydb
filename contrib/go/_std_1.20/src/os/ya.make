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

IF (OS_DARWIN)
    SRCS(
        dir_darwin.go
        exec_unix.go
        executable_darwin.go
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
ENDIF()

IF (OS_LINUX)
    SRCS(
        dir_unix.go
        dirent_linux.go
        exec_unix.go
        executable_procfs.go
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
ENDIF()

END()

RECURSE(
    exec
    signal
    user
)
