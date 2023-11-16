GO_LIBRARY()

SRCS(
    counters_supported.go
    coverage.go
    encoding.go
    fuzz.go
    mem.go
    minimize.go
    mutator.go
    mutators_byteslice.go
    pcg.go
    queue.go
    trace.go
    worker.go
)

IF (OS_DARWIN)
    SRCS(
        sys_posix.go
    )
ENDIF()

IF (OS_LINUX)
    SRCS(
        sys_posix.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        sys_windows.go
    )
ENDIF()

END()
