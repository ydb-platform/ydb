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
    bit_block_counter.go
    bit_run_reader.go
    bit_set_run_reader.go
    bitmap_generate.go
)

GO_XTEST_SRCS(
    bit_block_counter_test.go
    bit_run_reader_test.go
    bit_set_run_reader_test.go
    bitmap_generate_test.go
)

END()
