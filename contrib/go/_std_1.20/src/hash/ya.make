GO_LIBRARY()

SRCS(
    hash.go
)

END()

RECURSE(
    adler32
    crc32
    crc64
    fnv
    maphash
)
