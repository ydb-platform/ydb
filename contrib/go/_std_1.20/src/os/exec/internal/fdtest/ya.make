GO_LIBRARY()

IF (OS_DARWIN)
    SRCS(
        exists_unix.go
    )
ENDIF()

IF (OS_LINUX)
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
