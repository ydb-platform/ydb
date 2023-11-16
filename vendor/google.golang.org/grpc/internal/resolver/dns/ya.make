GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(dns_resolver.go)

GO_TEST_SRCS(dns_resolver_test.go)

END()

RECURSE(gotest)
