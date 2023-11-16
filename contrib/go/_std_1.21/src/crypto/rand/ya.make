GO_LIBRARY()

SRCS(
    rand.go
    util.go
)

GO_TEST_SRCS(rand_test.go)

GO_XTEST_SRCS(
    example_test.go
    util_test.go
)

IF (OS_LINUX)
    SRCS(
        rand_getrandom.go
        rand_unix.go
    )

    GO_TEST_SRCS(rand_batched_test.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(
        rand_getentropy.go
        rand_unix.go
    )

    GO_TEST_SRCS(rand_batched_test.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        rand_windows.go
    )
ENDIF()

END()

RECURSE(
)
