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
    app_version.go
    column_chunk.go
    file.go
    row_group.go
    statistics.go
    statistics_types.gen.go
)

GO_TEST_SRCS(stat_compare_test.go)

GO_XTEST_SRCS(
    metadata_test.go
    statistics_test.go
)

END()
