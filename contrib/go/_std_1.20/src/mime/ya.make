GO_LIBRARY()

SRCS(
    encodedword.go
    grammar.go
    mediatype.go
    type.go
)

IF (OS_DARWIN)
    SRCS(
        type_unix.go
    )
ENDIF()

IF (OS_LINUX)
    SRCS(
        type_unix.go
    )
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
