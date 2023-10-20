GO_LIBRARY()

SRCS(
    columnar_buffer_arrow_ipc_streaming.go
    columnar_buffer_factory.go
    doc.go
    read_limiter.go
    writer.go
)

GO_TEST_SRCS(
    time_test.go
)

END()
