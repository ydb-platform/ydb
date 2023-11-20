GO_LIBRARY()

LICENSE(MIT)

SRCS(
    doc.go
    float8.go
    int.go
    text.go
    timestamp.go
    timestamptz.go
    uuid.go
    zeronull.go
)

GO_XTEST_SRCS(
    # float8_test.go
    # int_test.go
    # text_test.go
    # timestamp_test.go
    # timestamptz_test.go
    # uuid_test.go
    # zeronull_test.go
)

END()

RECURSE(gotest)
