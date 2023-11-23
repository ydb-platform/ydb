GO_LIBRARY()

SRCS(
    common.go
    format.go
    reader.go
    strconv.go
    writer.go
)

GO_TEST_SRCS(
    fuzz_test.go
    reader_test.go
    strconv_test.go
    tar_test.go
    writer_test.go
)

GO_XTEST_SRCS(example_test.go)

IF (OS_LINUX)
    SRCS(
        stat_actime1.go
        stat_unix.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        stat_actime2.go
        stat_unix.go
    )
ENDIF()

END()

RECURSE(
)
