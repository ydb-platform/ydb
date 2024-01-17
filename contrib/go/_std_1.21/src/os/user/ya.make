GO_LIBRARY()

NO_COMPILER_WARNINGS()

SRCS(
    lookup.go
    user.go
)

IF (OS_DARWIN)
    SRCS(
        cgo_listgroups_unix.go
        cgo_lookup_syscall.go
        cgo_lookup_unix.go
        getgrouplist_syscall.go
    )
ENDIF()

IF (CGO_ENABLED)
    IF (OS_LINUX)
        SRCS(
            cgo_listgroups_unix.go
            cgo_lookup_unix.go
        )
        CGO_SRCS(
            cgo_lookup_cgo.go
            getgrouplist_unix.go
        )
    ENDIF()
ELSE()
    IF (OS_LINUX)
        SRCS(
            listgroups_unix.go
            lookup_stubs.go
            lookup_unix.go
        )
    ENDIF()
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        lookup_windows.go
    )
ENDIF()

END()
