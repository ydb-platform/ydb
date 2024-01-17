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

GO_TEST_SRCS(
    export_test.go
    helper_test.go
    helperfuncs_test.go
    match_test.go
    sub_test.go
)

GO_XTEST_SRCS(
    allocs_test.go
    benchmark_test.go
    flag_test.go
    panic_test.go
    testing_test.go
)

IF (OS_LINUX)
    SRCS(
        testing_other.go
    )
ENDIF()

IF (OS_DARWIN)
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
    slogtest
)
