GO_LIBRARY()

SRCS(
    hash.go
)

GO_XTEST_SRCS(
    example_test.go
    marshal_test.go
)

END()

RECURSE(
    adler32
    crc32
    crc64
    fnv
    maphash
)
