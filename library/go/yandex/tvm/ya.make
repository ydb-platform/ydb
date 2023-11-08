GO_LIBRARY()

SRCS(
    client.go
    context.go
    errors.go
    roles.go
    roles_entities_index.go
    roles_entities_index_builder.go
    roles_opts.go
    roles_parser.go
    roles_parser_opts.go
    roles_types.go
    service_ticket.go
    tvm.go
    user_ticket.go
)

GO_TEST_SRCS(
    roles_entities_index_builder_test.go
    roles_entities_index_test.go
    roles_parser_test.go
    roles_test.go
)

GO_XTEST_SRCS(tvm_test.go)

END()

RECURSE(examples)

RECURSE_FOR_TESTS(
    cachedtvm
    gotest
    mocks
    tvmauth
    tvmtool
)
