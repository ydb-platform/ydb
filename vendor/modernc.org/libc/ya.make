GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    ccgo.go
    etc.go
    fsync.go
    int128.go
    libc.go
    libc64.go
    mem.go
    nodmesg.go
    printf.go
    probes.go
    pthread.go
    pthread_all.go
    scanf.go
    sync.go
    watch.go
)

GO_TEST_SRCS(all_test.go)

IF (OS_LINUX)
    SRCS(
        ioutil_linux.go
        libc_linux.go
        libc_unix.go
    )
ENDIF()

IF (OS_LINUX AND ARCH_X86_64)
    SRCS(
        capi_linux_amd64.go
        libc_linux_amd64.go
        musl_linux_amd64.go
    )
ENDIF()

IF (OS_LINUX AND ARCH_ARM64)
    SRCS(
        capi_linux_arm64.go
        libc_linux_arm64.go
        musl_linux_arm64.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        ioutil_darwin.go
        libc_darwin.go
        libc_unix.go
    )
ENDIF()

IF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
        capi_darwin_amd64.go
        libc_darwin_amd64.go
        musl_darwin_amd64.go
    )
ENDIF()

IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
        capi_darwin_arm64.go
        libc_darwin_arm64.go
        musl_darwin_arm64.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(libc_windows.go)
ENDIF()

IF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
        capi_windows_amd64.go
        libc_windows_amd64.go
        musl_windows_amd64.go
    )
ENDIF()

IF (OS_WINDOWS AND ARCH_ARM64)
    SRCS(
        capi_windows_arm64.go
        libc_windows_arm64.go
        musl_windows_arm64.go
    )
ENDIF()

END()

RECURSE(
    gotest
    honnef.co
    netinet
    sys
    uuid
)

IF (OS_LINUX AND ARCH_X86_64)
    RECURSE(
        fts
        stdlib
        unistd
        pwd
        time
        utime
        stdio
        netdb
        poll
        fcntl
        limits
        termios
        grp
        signal
        errno
        langinfo
        pthread
        wctype
    )
ENDIF()

IF (OS_LINUX AND ARCH_ARM64)
    RECURSE(
        fts
        stdlib
        unistd
        pwd
        time
        utime
        stdio
        netdb
        poll
        fcntl
        limits
        termios
        grp
        signal
        errno
        langinfo
        pthread
        wctype
    )
ENDIF()

IF (OS_DARWIN AND ARCH_X86_64)
    RECURSE(
        fts
        stdlib
        unistd
        pwd
        time
        utime
        stdio
        netdb
        poll
        fcntl
        limits
        termios
        grp
        signal
        errno
        langinfo
        pthread
        wctype
    )
ENDIF()

IF (OS_DARWIN AND ARCH_ARM64)
    RECURSE(
        fts
        stdlib
        unistd
        pwd
        time
        utime
        stdio
        netdb
        poll
        fcntl
        limits
        termios
        grp
        signal
        errno
        langinfo
        pthread
        wctype
    )
ENDIF()

IF (OS_WINDOWS)
    RECURSE(
        stdlib
        unistd
        time
        utime
        stdio
        fcntl
        limits
        signal
        errno
        pthread
        wctype
    )
ENDIF()
