GO_LIBRARY()

SRCS(
    description.go
    doc.go
    histogram.go
    sample.go
    value.go
)

GO_XTEST_SRCS(
    description_test.go
    example_test.go
)

END()

RECURSE(
)
