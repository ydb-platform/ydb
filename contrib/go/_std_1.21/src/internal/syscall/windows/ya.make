GO_LIBRARY()

BUILD_ONLY_IF(
    WARNING
    OS_WINDOWS
)

IF (OS_WINDOWS)
    SRCS(
        memory_windows.go
        net_windows.go
        psapi_windows.go
        reparse_windows.go
        security_windows.go
        symlink_windows.go
        syscall_windows.go
        zsyscall_windows.go
    )

    GO_XTEST_SRCS(exec_windows_test.go)
ENDIF()

END()

RECURSE(
)

IF (OS_WINDOWS)
    RECURSE(
        registry
        sysdll
    )
ENDIF()
