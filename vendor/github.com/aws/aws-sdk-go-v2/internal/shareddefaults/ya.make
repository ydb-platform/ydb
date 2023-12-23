GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    shared_config.go
)

IF (OS_LINUX)
    GO_XTEST_SRCS(shared_config_other_test.go)
ENDIF()

IF (OS_DARWIN)
    GO_XTEST_SRCS(shared_config_other_test.go)
ENDIF()

IF (OS_WINDOWS)
    GO_XTEST_SRCS(shared_config_windows_test.go)
ENDIF()

END()

RECURSE(
    gotest
)
