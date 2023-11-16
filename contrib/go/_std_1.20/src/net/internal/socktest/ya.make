GO_LIBRARY()

SRCS(
    switch.go
    switch_posix.go
)

IF (OS_DARWIN)
    SRCS(
        switch_unix.go
        sys_unix.go
    )
ENDIF()

IF (OS_LINUX)
    SRCS(
        switch_unix.go
        sys_cloexec.go
        sys_unix.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        switch_windows.go
        sys_windows.go
    )
ENDIF()

END()
