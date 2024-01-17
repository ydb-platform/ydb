GO_LIBRARY()

BUILD_ONLY_IF(
    WARNING
    OS_WINDOWS
)

IF (OS_WINDOWS)
    SRCS(
        key.go
        syscall.go
        value.go
        zsyscall_windows.go
    )

    GO_TEST_SRCS(export_test.go)

    GO_XTEST_SRCS(registry_test.go)
ENDIF()

END()

RECURSE(
)
