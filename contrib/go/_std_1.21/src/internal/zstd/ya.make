GO_LIBRARY()

SRCS(
    bits.go
    block.go
    fse.go
    huff.go
    literals.go
    xxhash.go
    zstd.go
)

GO_TEST_SRCS(
    fse_test.go
    fuzz_test.go
    xxhash_test.go
    zstd_test.go
)

END()

RECURSE(
)
