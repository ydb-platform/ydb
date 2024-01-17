GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    alts.go
    utils.go
)

IF (OS_LINUX)
    GO_TEST_SRCS(
        alts_test.go
        utils_test.go
    )
ENDIF()

IF (OS_WINDOWS)
    GO_TEST_SRCS(
        alts_test.go
        utils_test.go
    )
ENDIF()

END()

RECURSE(
    gotest
    internal
    # yo
)
