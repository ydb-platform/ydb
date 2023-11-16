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
    sql_batch_reader.go
    sqlite_info.go
    sqlite_server.go
    sqlite_tables_schema_batch_reader.go
    type_info.go
)

END()

RECURSE(cmd)
