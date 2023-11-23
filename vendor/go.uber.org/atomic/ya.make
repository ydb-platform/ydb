GO_LIBRARY()

LICENSE(MIT)

SRCS(
    bool.go
    bool_ext.go
    doc.go
    duration.go
    duration_ext.go
    error.go
    error_ext.go
    float32.go
    float32_ext.go
    float64.go
    float64_ext.go
    gen.go
    int32.go
    int64.go
    nocmp.go
    pointer_go118.go
    pointer_go119.go
    string.go
    string_ext.go
    time.go
    time_ext.go
    uint32.go
    uint64.go
    uintptr.go
    unsafe_pointer.go
    value.go
)

GO_TEST_SRCS(
    assert_test.go
    bool_test.go
    duration_test.go
    error_test.go
    float32_test.go
    float64_test.go
    int32_test.go
    int64_test.go
    nocmp_test.go
    pointer_test.go
    stress_test.go
    string_test.go
    time_test.go
    uint32_test.go
    uint64_test.go
    uintptr_test.go
    unsafe_pointer_test.go
    value_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(internal)

IF (NOT OPENSOURCE)
    RECURSE(gotest)
ENDIF()
