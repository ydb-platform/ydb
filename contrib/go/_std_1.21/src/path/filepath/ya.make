GO_LIBRARY()

SRCS(
    match.go
    path.go
    symlink.go
)

GO_TEST_SRCS(export_test.go)

GO_XTEST_SRCS(
    example_test.go
    match_test.go
    path_test.go
)

IF (OS_LINUX)
    SRCS(
        path_unix.go
        symlink_unix.go
    )

    GO_XTEST_SRCS(
        example_unix_test.go
        example_unix_walk_test.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        path_unix.go
        symlink_unix.go
    )

    GO_XTEST_SRCS(
        example_unix_test.go
        example_unix_walk_test.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        path_windows.go
        symlink_windows.go
    )

    GO_TEST_SRCS(export_windows_test.go)

    GO_XTEST_SRCS(path_windows_test.go)
ENDIF()

END()

RECURSE(
)
