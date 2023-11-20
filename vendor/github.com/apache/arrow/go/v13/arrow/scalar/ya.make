GO_LIBRARY()

LICENSE(
    Apache-2.0 AND
    BSD-2-Clause AND
    BSD-3-Clause AND
    BSL-1.0 AND
    CC-BY-3.0 AND
    HPND AND
    MIT AND
    NCSA AND
    OpenSSL AND
    Zlib
)

SRCS(
    append.go
    binary.go
    compare.go
    nested.go
    numeric.gen.go
    parse.go
    scalar.go
    temporal.go
)

GO_XTEST_SRCS(
    append_test.go
    numeric.gen_test.go
    scalar_test.go
)

END()
