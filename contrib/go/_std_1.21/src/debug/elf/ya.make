GO_LIBRARY()

SRCS(
    elf.go
    file.go
    reader.go
)

GO_TEST_SRCS(
    elf_test.go
    file_test.go
    symbols_test.go
)

END()

RECURSE(
)
