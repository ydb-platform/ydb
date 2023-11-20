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

GO_TEST_SRCS(
    export_test.go
    internal_test.go
)

GO_XTEST_SRCS(
    example_test.go
    format_test.go
    mono_test.go
    sleep_test.go
    tick_test.go
    time_test.go
    tzdata_test.go
    zoneinfo_test.go
)

IF (OS_LINUX)
    SRCS(
        sys_unix.go
        zoneinfo_unix.go
    )

    GO_XTEST_SRCS(zoneinfo_unix_test.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(
        sys_unix.go
        zoneinfo_unix.go
    )

    GO_XTEST_SRCS(zoneinfo_unix_test.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        sys_windows.go
        zoneinfo_abbrs_windows.go
        zoneinfo_windows.go
    )

    GO_TEST_SRCS(export_windows_test.go)

    GO_XTEST_SRCS(zoneinfo_windows_test.go)
ENDIF()

END()

RECURSE(
    tzdata
)
