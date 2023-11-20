GO_LIBRARY()

SRCS(
    switch.go
    switch_posix.go
)

GO_XTEST_SRCS(main_test.go)

IF (OS_LINUX)
    SRCS(
        switch_unix.go
        sys_cloexec.go
        sys_unix.go
    )

    GO_XTEST_SRCS(main_unix_test.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(
        switch_unix.go
        sys_unix.go
    )

    GO_XTEST_SRCS(main_unix_test.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        switch_windows.go
        sys_windows.go
    )

    GO_XTEST_SRCS(main_windows_test.go)
ENDIF()

END()

RECURSE(
)
