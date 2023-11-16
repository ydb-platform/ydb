GO_LIBRARY()

LICENSE(BSD-3-Clause)

BUILD_ONLY_IF(
    WARNING
    OS_DARWIN
    OS_LINUX
)

SRCS(
    endian_little.go
)

IF (OS_LINUX)
    SRCS(
        affinity_linux.go
        aliases.go
        bluetooth_linux.go
        constants.go
        dev_linux.go
        dirent.go
        env_unix.go
        fcntl.go
        fdset.go
        ifreq_linux.go
        ioctl_linux.go
        ioctl_unsigned.go
        mremap.go
        pagesize_unix.go
        race0.go
        readdirent_getdents.go
        sockcmsg_linux.go
        sockcmsg_unix.go
        sockcmsg_unix_other.go
        syscall.go
        syscall_linux.go
        syscall_linux_gc.go
        syscall_unix.go
        syscall_unix_gc.go
        sysvshm_linux.go
        sysvshm_unix.go
        timestruct.go
        zerrors_linux.go
        zsyscall_linux.go
        ztypes_linux.go
    )

    GO_TEST_SRCS(
        export_mremap_test.go
        ifreq_linux_test.go
        syscall_internal_linux_test.go
    )

    GO_XTEST_SRCS(
        creds_test.go
        dev_linux_test.go
        dirent_test.go
        dup3_test.go
        example_exec_test.go
        example_flock_test.go
        example_sysvshm_test.go
        fdset_test.go
        mmap_unix_test.go
        mremap_test.go
        pipe2_test.go
        sendfile_test.go
        syscall_linux_test.go
        syscall_test.go
        syscall_unix_test.go
        sysvshm_unix_test.go
        timestruct_test.go
        xattr_test.go
    )
ENDIF()

IF (OS_LINUX AND ARCH_X86_64)
    SRCS(
        asm_linux_amd64.s
        syscall_linux_alarm.go
        syscall_linux_amd64.go
        syscall_linux_amd64_gc.go
        zerrors_linux_amd64.go
        zptrace_x86_linux.go
        zsyscall_linux_amd64.go
        zsysnum_linux_amd64.go
        ztypes_linux_amd64.go
    )
ENDIF()

IF (OS_LINUX AND ARCH_ARM64)
    SRCS(
        asm_linux_arm64.s
        syscall_linux_arm64.go
        zerrors_linux_arm64.go
        zptrace_armnn_linux.go
        zptrace_linux_arm64.go
        zsyscall_linux_arm64.go
        zsysnum_linux_arm64.go
        ztypes_linux_arm64.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        aliases.go
        constants.go
        dev_darwin.go
        dirent.go
        env_unix.go
        fcntl_darwin.go
        fdset.go
        ioctl_unsigned.go
        mmap_nomremap.go
        pagesize_unix.go
        ptrace_darwin.go
        race0.go
        readdirent_getdirentries.go
        sockcmsg_unix.go
        sockcmsg_unix_other.go
        syscall.go
        syscall_bsd.go
        syscall_darwin.go
        syscall_darwin_libSystem.go
        syscall_unix.go
        syscall_unix_gc.go
        sysvshm_unix.go
        sysvshm_unix_other.go
        timestruct.go
    )

    GO_TEST_SRCS(
        darwin_test.go
        syscall_internal_bsd_test.go
        syscall_internal_darwin_test.go
    )

    GO_XTEST_SRCS(
        dirent_test.go
        example_exec_test.go
        example_flock_test.go
        example_sysvshm_test.go
        fdset_test.go
        getdirentries_test.go
        getfsstat_test.go
        mmap_unix_test.go
        syscall_bsd_test.go
        syscall_darwin_test.go
        syscall_test.go
        syscall_unix_test.go
        timestruct_test.go
        xattr_test.go
    )
ENDIF()

IF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
        asm_bsd_amd64.s
        syscall_darwin_amd64.go
        zerrors_darwin_amd64.go
        zsyscall_darwin_amd64.go
        zsyscall_darwin_amd64.s
        zsysnum_darwin_amd64.go
        ztypes_darwin_amd64.go
    )

    GO_TEST_SRCS(darwin_amd64_test.go)

    GO_XTEST_SRCS(
        sendfile_test.go
        sysvshm_unix_test.go
    )
ENDIF()

IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
        asm_bsd_arm64.s
        syscall_darwin_arm64.go
        zerrors_darwin_arm64.go
        zsyscall_darwin_arm64.go
        zsyscall_darwin_arm64.s
        zsysnum_darwin_arm64.go
        ztypes_darwin_arm64.go
    )

    GO_TEST_SRCS(darwin_arm64_test.go)
ENDIF()

END()

RECURSE(
    gotest
    internal
)
