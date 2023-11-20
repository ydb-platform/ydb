GO_LIBRARY()

LICENSE(MIT)

SRCS(
    unmarshal.go
    wkt.go
)

GO_TEST_SRCS(
    unmarshal_test.go
    wkt_test.go
)

END()

RECURSE(gotest)
