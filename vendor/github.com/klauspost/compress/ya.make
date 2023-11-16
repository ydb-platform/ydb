GO_LIBRARY()

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause AND
    MIT
)

SRCS(compressible.go)

GO_TEST_SRCS(compressible_test.go)

END()

RECURSE(
    flate
    fse
    gotest
    gzip
    huff0
    internal
    s2
    snappy
    zip
    zstd
)
