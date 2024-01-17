GO_LIBRARY()

SRCS(
    goos.go
)

IF (OS_LINUX)
    SRCS(
        unix.go
        zgoos_linux.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        unix.go
        zgoos_darwin.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        nonunix.go
        zgoos_windows.go
    )
ENDIF()

END()
