GO_LIBRARY()

SRCS(
    deflate.go
    deflatefast.go
    dict_decoder.go
    huffman_bit_writer.go
    huffman_code.go
    inflate.go
    token.go
)

GO_TEST_SRCS(
    deflate_test.go
    dict_decoder_test.go
    flate_test.go
    huffman_bit_writer_test.go
    inflate_test.go
    reader_test.go
    writer_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
)
