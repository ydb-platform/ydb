IF (CGO_ENABLED)
    GO_LIBRARY()

    PEERDIR(
        library/cpp/sanitizer/include
    )

    NO_COMPILER_WARNINGS()

    SRCS(
        abi_loong64.h
        abi_ppc64x.h
        abi_riscv64.h
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

    IF (ARCH_ARM6 OR ARCH_ARM7)
        SRCS(
            asm_arm.s
            gcc_arm.S
        )
    ENDIF()

    IF (OS_DARWIN)
        SRCS(
            callbacks_unix.go
            callbacks_traceback.go
            CGO_EXPORT gcc_fatalf.c
            CGO_EXPORT gcc_libinit_unix.c
            CGO_EXPORT gcc_setenv.c
            CGO_EXPORT gcc_traceback.c
            CGO_EXPORT gcc_unix.c
            CGO_EXPORT pthread_unix.c
            setenv.go
        )

        IF (ARCH_ARM64)
            CGO_LDFLAGS(
                -framework
                CoreFoundation
            )
        ENDIF()

        IF (ARCH_X86_64)
            CGO_LDFLAGS(
                -lpthread
            )
        ENDIF()
    ENDIF()

    IF (OS_LINUX)
        CGO_LDFLAGS(-lpthread -ldl -lresolv)

        SRCS(
            callbacks_unix.go
            callbacks_traceback.go
            CGO_EXPORT gcc_fatalf.c
            CGO_EXPORT gcc_libinit_unix.c
            CGO_EXPORT gcc_setenv.c
            CGO_EXPORT gcc_traceback.c
            CGO_EXPORT gcc_unix.c
            CGO_EXPORT pthread_unix.c
            linux.go
            CGO_EXPORT linux_syscall.c
            setenv.go
        )

        IF (ARCH_ARM64 OR ARCH_X86_64)
            SRCS(
                CGO_EXPORT gcc_sigaction.c
                CGO_EXPORT gcc_mmap.c
                mmap.go
                sigaction.go
            )
        ENDIF()
    ENDIF()

    IF (OS_ANDROID)
        CGO_LDFLAGS(-ldl -llog)

        SRCS(
            callbacks_unix.go
            callbacks_traceback.go
            CGO_EXPORT gcc_android.c
            CGO_EXPORT gcc_libinit_unix.c
            CGO_EXPORT gcc_setenv.c
            CGO_EXPORT gcc_traceback.c
            linux.go
            CGO_EXPORT linux_syscall.c
            setenv.go
        )

        IF (ARCH_ARM64 OR ARCH_X86_64)
            SRCS(
                CGO_EXPORT gcc_sigaction.c
                CGO_EXPORT gcc_mmap.c
                mmap.go
                sigaction.go
            )
        ENDIF()
    ENDIF()

    IF (OS_WINDOWS)
        SRCS(
            CGO_EXPORT gcc_libinit_windows.c
            windows.go
        )
    ENDIF()

    END()
ENDIF()
