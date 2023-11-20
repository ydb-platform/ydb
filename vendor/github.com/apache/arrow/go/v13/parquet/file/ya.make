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
    column_reader.go
    column_reader_types.gen.go
    column_writer.go
    column_writer_types.gen.go
    file_reader.go
    file_writer.go
    level_conversion.go
    page_reader.go
    page_writer.go
    record_reader.go
    row_group_reader.go
    row_group_writer.go
)

GO_TEST_SRCS(level_conversion_test.go)

GO_XTEST_SRCS(
    column_reader_test.go
    column_writer_test.go
    file_reader_test.go
    file_writer_test.go
    row_group_writer_test.go
)

IF (OS_LINUX)
    SRCS(file_reader_mmap.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(file_reader_mmap.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(file_reader_mmap_windows.go)
ENDIF()

END()
