GO_LIBRARY()

SRCS(
    columnar_buffer_arrow_ipc_streaming_default.go
    columnar_buffer_arrow_ipc_streaming_empty_columns.go
    columnar_buffer_factory.go
    doc.go
    interface.go
    mock.go
    read_limiter.go
    sink.go
    sink_string.go
    size.go
    traffic_tracker.go
)

GO_TEST_SRCS(
    size_test.go
    traffic_tracker_test.go
)

END()

RECURSE_FOR_TESTS(ut)
