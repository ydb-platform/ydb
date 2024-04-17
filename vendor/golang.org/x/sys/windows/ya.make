GO_LIBRARY()

LICENSE(BSD-3-Clause)

IF (OS_WINDOWS)
    SRCS(
        aliases.go
        dll_windows.go
        env_windows.go
        eventlog.go
        exec_windows.go
        memory_windows.go
        race0.go
        security_windows.go
        service.go
        setupapi_windows.go
        str.go
        syscall.go
        syscall_windows.go
        types_windows.go
        zerrors_windows.go
        zknownfolderids_windows.go
        zsyscall_windows.go
    )

    GO_XTEST_SRCS(
        env_windows_test.go
        syscall_test.go
        syscall_windows_test.go
    )
ENDIF()

IF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
        types_windows_amd64.go
    )
ENDIF()

IF (OS_WINDOWS AND ARCH_ARM64)
    SRCS(
        types_windows_arm64.go
    )
ENDIF()

END()

RECURSE(
    gotest
    mkwinsyscall
)

IF (OS_WINDOWS)
    RECURSE(
        svc
        registry
    )
ENDIF()
