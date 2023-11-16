GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    grpclb.go
    grpclb_config.go
    grpclb_picker.go
    grpclb_remote_balancer.go
    grpclb_util.go
)

GO_TEST_SRCS(
    grpclb_config_test.go
    grpclb_test.go
    grpclb_util_test.go
)

END()

RECURSE(
    gotest
    grpc_lb_v1
    state
    # yo
)
