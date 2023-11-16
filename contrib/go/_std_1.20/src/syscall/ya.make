GO_LIBRARY()

SRCS(
    asan0.go
    endian_little.go
    msan0.go
    net.go
    syscall.go
    time_nofake.go
)

IF (OS_DARWIN)
    SRCS(
        bpf_darwin.go
        dirent.go
        env_unix.go
        exec_libc2.go
        exec_unix.go
        flock_darwin.go
        forkpipe.go
        ptrace_darwin.go
        rlimit.go
        rlimit_darwin.go
        route_bsd.go
        route_darwin.go
        sockcmsg_unix.go
        sockcmsg_unix_other.go
        syscall_bsd.go
        syscall_darwin.go
        syscall_unix.go
        timestruct.go
    )

    IF (ARCH_ARM64)
        SRCS(
            asm_darwin_arm64.s
            syscall_darwin_arm64.go
            zerrors_darwin_arm64.go
            zsyscall_darwin_arm64.go
            zsyscall_darwin_arm64.s
            zsysnum_darwin_arm64.go
            ztypes_darwin_arm64.go
        )
    ENDIF()

    IF (ARCH_X86_64)
        SRCS(
            asm_darwin_amd64.s
            syscall_darwin_amd64.go
            zerrors_darwin_amd64.go
            zsyscall_darwin_amd64.go
            zsyscall_darwin_amd64.s
            zsysnum_darwin_amd64.go
            ztypes_darwin_amd64.go
        )
    ENDIF()
ENDIF()

IF (OS_LINUX)
    SRCS(
        dirent.go
        env_unix.go
        exec_linux.go
        exec_unix.go
        flock.go
        lsf_linux.go
        netlink_linux.go
        rlimit.go
        rlimit_stub.go
        setuidgid_linux.go
        sockcmsg_linux.go
        sockcmsg_unix.go
        sockcmsg_unix_other.go
        syscall_linux.go
        syscall_linux_accept4.go
        syscall_unix.go
        timestruct.go
    )

    IF (ARCH_ARM64)
        SRCS(
            asm_linux_arm64.s
            syscall_linux_arm64.go
            zerrors_linux_arm64.go
            zsyscall_linux_arm64.go
            zsysnum_linux_arm64.go
            ztypes_linux_arm64.go
        )
    ENDIF()

    IF (ARCH_X86_64)
        SRCS(
            asm_linux_amd64.s
            syscall_linux_amd64.go
            zerrors_linux_amd64.go
            zsyscall_linux_amd64.go
            zsysnum_linux_amd64.go
            ztypes_linux_amd64.go
        )
    ENDIF()
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        dll_windows.go
        env_windows.go
        exec_windows.go
        security_windows.go
        syscall_windows.go
        types_windows.go
        zerrors_windows.go
        zsyscall_windows.go
    )

    IF (ARCH_ARM64)
        SRCS(
            types_windows_arm64.go
        )
    ENDIF()

    IF (ARCH_X86_64)
        SRCS(
            types_windows_amd64.go
        )
    ENDIF()
ENDIF()

END()
