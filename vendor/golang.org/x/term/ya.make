GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    term.go
    terminal.go
)

GO_TEST_SRCS(terminal_test.go)

GO_XTEST_SRCS(term_test.go)

IF (OS_LINUX)
    SRCS(
        term_unix.go
        term_unix_other.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        term_unix.go
        term_unix_bsd.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        term_windows.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
