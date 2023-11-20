GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    attributes.go
    authority.go
    client.go
    client_new.go
    clientimpl.go
    clientimpl_authority.go
    clientimpl_dump.go
    clientimpl_loadreport.go
    clientimpl_watchers.go
    logging.go
    requests_counter.go
    singleton.go
)

GO_TEST_SRCS(
    # authority_test.go
    # client_test.go
    # loadreport_test.go
    # requests_counter_test.go
    # singleton_test.go
)

GO_XTEST_SRCS(
    # xdsclient_test.go
)

END()

RECURSE(
    bootstrap
    # e2e_test
    # gotest
    load
    tests
    transport
    xdslbregistry
    xdsresource
)
