GO_LIBRARY()

SRCS(
    doc.go
    sig.s
    signal.go
    signal_unix.go
)

GO_XTEST_SRCS(example_test.go)

IF (OS_LINUX)
    GO_TEST_SRCS(
        signal_linux_test.go
        signal_test.go
    )

    GO_XTEST_SRCS(
        example_unix_test.go
        signal_cgo_test.go
    )
ENDIF()

IF (OS_DARWIN)
    GO_TEST_SRCS(signal_test.go)

    GO_XTEST_SRCS(
        example_unix_test.go
        signal_cgo_test.go
    )
ENDIF()

IF (OS_WINDOWS)
    GO_TEST_SRCS(signal_windows_test.go)
ENDIF()

END()

RECURSE(
)
