GO_LIBRARY()
IF (FALSE)
    MESSAGE(FATAL this shall never happen)

ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		deflate.go
		deflatefast.go
		dict_decoder.go
		huffman_bit_writer.go
		huffman_code.go
		inflate.go
		token.go
    )
ELSEIF (OS_LINUX AND ARCH_ARM64)
    SRCS(
		deflate.go
		deflatefast.go
		dict_decoder.go
		huffman_bit_writer.go
		huffman_code.go
		inflate.go
		token.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		deflate.go
		deflatefast.go
		dict_decoder.go
		huffman_bit_writer.go
		huffman_code.go
		inflate.go
		token.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		deflate.go
		deflatefast.go
		dict_decoder.go
		huffman_bit_writer.go
		huffman_code.go
		inflate.go
		token.go
    )
ELSEIF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		deflate.go
		deflatefast.go
		dict_decoder.go
		huffman_bit_writer.go
		huffman_code.go
		inflate.go
		token.go
    )
ELSEIF (OS_DARWIN AND ARCH_AARCH64)
    SRCS(
		deflate.go
		deflatefast.go
		dict_decoder.go
		huffman_bit_writer.go
		huffman_code.go
		inflate.go
		token.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		deflate.go
		deflatefast.go
		dict_decoder.go
		huffman_bit_writer.go
		huffman_code.go
		inflate.go
		token.go
    )
ELSEIF (OS_WINDOWS AND ARCH_ARM64)
    SRCS(
		deflate.go
		deflatefast.go
		dict_decoder.go
		huffman_bit_writer.go
		huffman_code.go
		inflate.go
		token.go
    )
ELSEIF (OS_WINDOWS AND ARCH_AARCH64)
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
