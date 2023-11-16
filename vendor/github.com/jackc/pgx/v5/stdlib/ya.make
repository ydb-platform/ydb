GO_LIBRARY()

LICENSE(MIT)

SRCS(sql.go)

GO_XTEST_SRCS(
    # bench_test.go
    # sql_test.go
)

END()

RECURSE(gotest)
