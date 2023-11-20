GO_LIBRARY()

SRCS(
    encodedword.go
    grammar.go
    mediatype.go
    type.go
)

GO_TEST_SRCS(
    encodedword_test.go
    mediatype_test.go
    type_test.go
)

GO_XTEST_SRCS(example_test.go)

IF (OS_LINUX)
    SRCS(
        type_unix.go
    )

    GO_TEST_SRCS(type_unix_test.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(
        type_unix.go
    )

    GO_TEST_SRCS(type_unix_test.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        type_windows.go
    )
ENDIF()

END()

RECURSE(
    multipart
    quotedprintable
)
