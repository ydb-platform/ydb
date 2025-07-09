GO_LIBRARY()

LICENSE(MIT)

VERSION(v1.5.1)

SRCS(
    validator_field.go
)

GO_XTEST_SRCS(validator_field_test.go)

END()

RECURSE(
    gotest
)
