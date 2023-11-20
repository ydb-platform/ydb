GO_LIBRARY()

SRCS(
    buffer.go
    bytes.go
    reader.go
)

GO_TEST_SRCS(export_test.go)

GO_XTEST_SRCS(
    buffer_test.go
    bytes_test.go
    compare_test.go
    example_test.go
    reader_test.go
)

IF (OS_LINUX)
    GO_XTEST_SRCS(boundary_test.go)
ENDIF()

END()

RECURSE(
)
