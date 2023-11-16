GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    parse.go
    readfile.go
    valueparser.go
)

GO_XTEST_SRCS(valueparser_test.go)

IF (OS_LINUX)
    SRCS(
        sysreadfile.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        sysreadfile.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        sysreadfile_compat.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
