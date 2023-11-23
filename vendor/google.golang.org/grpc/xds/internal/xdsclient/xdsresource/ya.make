GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    cluster_resource_type.go
    endpoints_resource_type.go
    errors.go
    filter_chain.go
    listener_resource_type.go
    logging.go
    matcher.go
    matcher_path.go
    name.go
    resource_type.go
    route_config_resource_type.go
    type.go
    type_cds.go
    type_eds.go
    type_lds.go
    type_rds.go
    unmarshal_cds.go
    unmarshal_eds.go
    unmarshal_lds.go
    unmarshal_rds.go
)

GO_TEST_SRCS(
    filter_chain_test.go
    matcher_path_test.go
    matcher_test.go
    name_test.go
    test_utils_test.go
    unmarshal_cds_test.go
    unmarshal_eds_test.go
    unmarshal_lds_test.go
    unmarshal_rds_test.go
)

END()

RECURSE(
    gotest
    tests
    version
)
