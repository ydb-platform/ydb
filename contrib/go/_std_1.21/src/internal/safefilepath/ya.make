GO_LIBRARY()

SRCS(
    path.go
)

GO_XTEST_SRCS(path_test.go)

IF (OS_LINUX)
    SRCS(
        path_other.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        path_other.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        path_windows.go
    )
ENDIF()

END()

RECURSE(
)
