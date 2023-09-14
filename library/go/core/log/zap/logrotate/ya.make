GO_LIBRARY()

SRCS(error.go)

IF (OS_LINUX)
    SRCS(sink.go)

    GO_TEST_SRCS(sink_test.go)

    GO_XTEST_SRCS(example_sink_test.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(sink.go)

    GO_TEST_SRCS(sink_test.go)

    GO_XTEST_SRCS(example_sink_test.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(sink_stub.go)
ENDIF()

END()

IF (OS_DARWIN OR OS_FREEBSD OR OS_LINUX)
    RECURSE_FOR_TESTS(gotest)
ENDIF()
