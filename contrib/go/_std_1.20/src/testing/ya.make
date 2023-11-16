GO_LIBRARY()

SRCS(
    allocs.go
    benchmark.go
    cover.go
    example.go
    fuzz.go
    match.go
    newcover.go
    run_example.go
    testing.go
)

IF (OS_DARWIN)
    SRCS(
        testing_other.go
    )
ENDIF()

IF (OS_LINUX)
    SRCS(
        testing_other.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        testing_windows.go
    )
ENDIF()

END()

RECURSE(
    fstest
    internal
    iotest
    quick
)
