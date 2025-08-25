GO_LIBRARY()
IF (TRUE)
    SRCS(
        deflate.go
        deflatefast.go
        dict_decoder.go
        huffman_bit_writer.go
        huffman_code.go
        inflate.go
        token.go
    )
ENDIF()
END()
