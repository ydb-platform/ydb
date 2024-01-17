GO_TEST()

LICENSE(Apache-2.0)

GO_SKIP_TESTS(
    Test
)

GO_XTEST_SRCS(
    authority_test.go
    cds_watchers_test.go
    dump_test.go
    eds_watchers_test.go
    federation_watchers_test.go
    lds_watchers_test.go
    misc_watchers_test.go
    rds_watchers_test.go
    resource_update_test.go
)

END()
