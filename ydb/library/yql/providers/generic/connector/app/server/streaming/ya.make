GO_LIBRARY()

SRCS(
    doc.go
    streamer.go
)

GO_TEST_SRCS(
    streamer_test.go
)

END()

RECURSE_FOR_TESTS(ut)
