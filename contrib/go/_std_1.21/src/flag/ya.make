GO_LIBRARY()

SRCS(
    flag.go
)

GO_TEST_SRCS(export_test.go)

GO_XTEST_SRCS(
    example_func_test.go
    example_test.go
    example_textvar_test.go
    example_value_test.go
    flag_test.go
)

END()

RECURSE(
)
