GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    common.go
    decimal.go
    format.go
    number.go
    pattern.go
    roundingmode_string.go
    tables.go
)

GO_TEST_SRCS(
    decimal_test.go
    format_test.go
    number_test.go
    pattern_test.go
    tables_test.go
)

END()

RECURSE(
    gotest
)
