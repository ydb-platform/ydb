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
    client.go
    column_metadata.go
    server.go
    sql_info.go
    types.go
)

GO_XTEST_SRCS(
    client_test.go
    server_test.go
    sqlite_server_test.go
)

END()

RECURSE(
    driver
    example
    schema_ref
)
