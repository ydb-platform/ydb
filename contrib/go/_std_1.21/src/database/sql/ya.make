GO_LIBRARY()

SRCS(
    convert.go
    ctxutil.go
    sql.go
)

GO_TEST_SRCS(
    convert_test.go
    fakedb_test.go
    sql_test.go
)

GO_XTEST_SRCS(
    example_cli_test.go
    example_service_test.go
    example_test.go
)

END()

RECURSE(
    driver
)
