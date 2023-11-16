GO_LIBRARY()

LICENSE(MIT)

SRCS(
    big.go
    bigbytes.go
    bytes.go
    comma.go
    commaf.go
    ftoa.go
    humanize.go
    number.go
    ordinals.go
    si.go
    times.go
)

GO_TEST_SRCS(
    bigbytes_test.go
    bytes_test.go
    comma_test.go
    commaf_test.go
    common_test.go
    ftoa_test.go
    number_test.go
    ordinals_test.go
    si_test.go
    times_test.go
)

END()

RECURSE(
    english
    gotest
)
