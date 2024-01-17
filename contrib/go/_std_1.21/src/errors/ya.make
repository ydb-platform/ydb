GO_LIBRARY()

SRCS(
    errors.go
    join.go
    wrap.go
)

GO_XTEST_SRCS(
    errors_test.go
    example_test.go
    join_test.go
    wrap_test.go
)

END()

RECURSE(
)
