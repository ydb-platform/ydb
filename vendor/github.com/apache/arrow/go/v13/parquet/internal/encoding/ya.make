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
    boolean_decoder.go
    boolean_encoder.go
    byte_array_decoder.go
    byte_array_encoder.go
    decoder.go
    delta_bit_packing.go
    delta_byte_array.go
    delta_length_byte_array.go
    encoder.go
    fixed_len_byte_array_decoder.go
    fixed_len_byte_array_encoder.go
    levels.go
    memo_table.go
    memo_table_types.gen.go
    plain_encoder_types.gen.go
    typed_encoder.gen.go
    types.go
)

GO_XTEST_SRCS(
    encoding_benchmarks_test.go
    encoding_test.go
    levels_test.go
    memo_table_test.go
)

END()
