GO_LIBRARY()

SRCS(
    connection_manager.go
    doc.go
    sql_formatter.go
    type_mapper.go
)

GO_TEST_SRCS(
    sql_formatter_test.go
)

END()

RECURSE_FOR_TESTS(
    ut
)
