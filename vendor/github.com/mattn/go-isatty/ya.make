GO_LIBRARY()

LICENSE(MIT)

SRCS(doc.go)

GO_XTEST_SRCS(example_test.go)

IF (OS_LINUX)
    SRCS(isatty_tcgets.go)

    GO_TEST_SRCS(isatty_others_test.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(isatty_bsd.go)

    GO_TEST_SRCS(isatty_others_test.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(isatty_windows.go)

    GO_TEST_SRCS(isatty_windows_test.go)
ENDIF()

END()

RECURSE(gotest)
