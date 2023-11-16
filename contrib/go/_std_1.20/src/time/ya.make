GO_LIBRARY()

SRCS(
    format.go
    format_rfc3339.go
    sleep.go
    tick.go
    time.go
    zoneinfo.go
    zoneinfo_goroot.go
    zoneinfo_read.go
)

IF (OS_DARWIN)
    SRCS(
        sys_unix.go
        zoneinfo_unix.go
    )
ENDIF()

IF (OS_LINUX)
    SRCS(
        sys_unix.go
        zoneinfo_unix.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        sys_windows.go
        zoneinfo_abbrs_windows.go
        zoneinfo_windows.go
    )
ENDIF()

END()

RECURSE(
    tzdata
)
