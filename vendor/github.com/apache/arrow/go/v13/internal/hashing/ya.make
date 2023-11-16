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
    hash_funcs.go
    hash_string_go1.19.go
    xxh3_memo_table.gen.go
    xxh3_memo_table.go
)

GO_TEST_SRCS(hashing_test.go)

END()
