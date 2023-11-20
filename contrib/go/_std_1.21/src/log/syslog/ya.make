GO_LIBRARY()

SRCS(
    doc.go
)

IF (OS_LINUX)
    SRCS(
        syslog.go
        syslog_unix.go
    )

    GO_TEST_SRCS(syslog_test.go)

    GO_XTEST_SRCS(example_test.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(
        syslog.go
        syslog_unix.go
    )

    GO_TEST_SRCS(syslog_test.go)

    GO_XTEST_SRCS(example_test.go)
ENDIF()

END()

RECURSE(
)
