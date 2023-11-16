GO_LIBRARY()

SRCS(
    path.go
)

IF (OS_DARWIN)
    SRCS(
        path_other.go
    )
ENDIF()

IF (OS_LINUX)
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
