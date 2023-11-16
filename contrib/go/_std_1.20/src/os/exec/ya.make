GO_LIBRARY()

SRCS(
    exec.go
)

IF (OS_DARWIN)
    SRCS(
        exec_unix.go
        lp_unix.go
    )
ENDIF()

IF (OS_LINUX)
    SRCS(
        exec_unix.go
        lp_unix.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        exec_windows.go
        lp_windows.go
    )
ENDIF()

END()

RECURSE(
    internal
)
