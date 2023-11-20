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

GO_TEST_SRCS(
    encoding_test.go
    minimize_test.go
    mutator_test.go
    mutators_byteslice_test.go
    queue_test.go
    worker_test.go
)

IF (OS_LINUX)
    SRCS(
        sys_posix.go
    )
ENDIF()

IF (OS_DARWIN)
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

RECURSE(
)
