IF (CGO_ENABLED)
    GO_LIBRARY()

    PEERDIR(
        library/cpp/sanitizer/include
    )

    NO_COMPILER_WARNINGS()

    SRCS(
        abi_loong64.h
        abi_ppc64x.h
        callbacks.go
        CGO_EXPORT gcc_context.c
        CGO_EXPORT gcc_util.c
        handle.go
        iscgo.go
        libcgo.h
        libcgo_unix.h
    )

    CGO_SRCS(
        cgo.go
    )

    IF (ARCH_ARM64)
        SRCS(
            abi_arm64.h
            asm_arm64.s
            gcc_arm64.S
        )
    ENDIF()

    IF (ARCH_X86_64)
        SRCS(
            abi_amd64.h
            asm_amd64.s
            gcc_amd64.S
        )
    ENDIF()

    IF (OS_DARWIN)
        SRCS(
            callbacks_traceback.go
            CGO_EXPORT gcc_libinit.c
            CGO_EXPORT gcc_setenv.c
            CGO_EXPORT gcc_stack_darwin.c
            CGO_EXPORT gcc_traceback.c
            setenv.go
        )

        IF (ARCH_ARM64)
            CGO_LDFLAGS(
                -framework
                CoreFoundation
            )

            SRCS(
                CGO_EXPORT gcc_darwin_arm64.c
            )
        ENDIF()

        IF (ARCH_X86_64)
            CGO_LDFLAGS(
                -lpthread
            )

            SRCS(
                CGO_EXPORT gcc_darwin_amd64.c
            )
        ENDIF()
    ENDIF()

    IF (OS_LINUX)
        CGO_LDFLAGS(-lpthread -ldl -lresolv)

        SRCS(
            callbacks_traceback.go
            CGO_EXPORT gcc_fatalf.c
            CGO_EXPORT gcc_libinit.c
            CGO_EXPORT gcc_mmap.c
            CGO_EXPORT gcc_setenv.c
            CGO_EXPORT gcc_sigaction.c
            CGO_EXPORT gcc_stack_unix.c
            CGO_EXPORT gcc_traceback.c
            linux.go
            CGO_EXPORT linux_syscall.c
            mmap.go
            setenv.go
            sigaction.go
        )

        IF (ARCH_ARM64)
            SRCS(
                CGO_EXPORT gcc_linux_arm64.c
            )
        ENDIF()

        IF (ARCH_X86_64)
            SRCS(
                CGO_EXPORT gcc_linux_amd64.c
            )
        ENDIF()
    ENDIF()

    IF (OS_WINDOWS)
        SRCS(
            CGO_EXPORT gcc_libinit_windows.c
            CGO_EXPORT gcc_stack_windows.c
            libcgo_windows.h
        )

        IF (ARCH_ARM64)
            SRCS(
                CGO_EXPORT gcc_windows_arm64.c
            )
        ENDIF()

        IF (ARCH_X86_64)
            SRCS(
                CGO_EXPORT gcc_windows_amd64.c
            )
        ENDIF()
    ENDIF()

    END()
ENDIF()
