GO_LIBRARY()

BUILD_ONLY_IF(WARNING OS_DARWIN OR OS_LINUX)

IF (OS_DARWIN)
    CGO_LDFLAGS(
        -lresolv
    )

    SRCS(
        asm_darwin.s
        at_libc2.go
        at_sysnum_darwin.go
        constants.go
        eaccess_other.go
        fcntl_unix.go
        getentropy_darwin.go
        kernel_version_other.go
        net.go
        net_darwin.go
        nonblocking_unix.go
        pty_darwin.go
        user_darwin.go
    )
ENDIF()

IF (OS_LINUX)
    SRCS(
        at.go
        at_fstatat.go
        at_sysnum_linux.go
        constants.go
        copy_file_range_linux.go
        eaccess_linux.go
        fcntl_unix.go
        getrandom.go
        getrandom_linux.go
        kernel_version_linux.go
        net.go
        nonblocking_unix.go
    )

    IF (ARCH_ARM64)
        SRCS(
            at_sysnum_fstatat_linux.go
            sysnum_linux_generic.go
        )
    ENDIF()

    IF (ARCH_X86_64)
        SRCS(
            at_sysnum_newfstatat_linux.go
            sysnum_linux_amd64.go
        )
    ENDIF()
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        kernel_version_other.go
    )
ENDIF()

END()
