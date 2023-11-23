GO_LIBRARY()

LICENSE(MIT)

SRCS(
    128.go
    32.go
    64.go
    ch_128.go
    ch_64.go
    doc.go
)

GO_TEST_SRCS(
    ch_128_test.go
    ch_64_test.go
    city_test.go
    fuzz_test.go
)

GO_XTEST_SRCS(
    data_test.go
    example_test.go
)

GO_TEST_EMBED_PATTERN(_testdata/ch128.csv)

GO_XTEST_EMBED_PATTERN(_testdata/data.json)

END()

RECURSE(gotest)
