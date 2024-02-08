GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    funcs.go
    id.go
    logging.go
    types.go
)

IF (OS_LINUX)
    SRCS(
        types_linux.go
        util_linux.go
    )

    GO_XTEST_SRCS(util_test.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(
        types_nonlinux.go
        util_nonlinux.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        types_nonlinux.go
        util_nonlinux.go
    )
ENDIF()

END()

RECURSE(gotest)
