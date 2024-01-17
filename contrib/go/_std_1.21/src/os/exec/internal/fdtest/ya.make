GO_LIBRARY()

GO_TEST_SRCS(exists_test.go)

IF (OS_LINUX)
    SRCS(
        exists_unix.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        exists_unix.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        exists_windows.go
    )
ENDIF()

END()

RECURSE(
)
