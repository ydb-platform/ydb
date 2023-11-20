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
    compression.go
    endian_swap.go
    file_reader.go
    file_writer.go
    ipc.go
    message.go
    metadata.go
    reader.go
    writer.go
)

GO_TEST_SRCS(
    endian_swap_test.go
    message_test.go
    metadata_test.go
    reader_test.go
    writer_test.go
)

GO_XTEST_SRCS(
    file_test.go
    ipc_test.go
    stream_test.go
)

END()

RECURSE(cmd)
