GO_LIBRARY()

SRCS(
    match.go
    path.go
    symlink.go
)

IF (OS_DARWIN)
    SRCS(
        path_unix.go
        symlink_unix.go
    )
ENDIF()

IF (OS_LINUX)
    SRCS(
        path_unix.go
        symlink_unix.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        path_windows.go
        symlink_windows.go
    )
ENDIF()

END()
