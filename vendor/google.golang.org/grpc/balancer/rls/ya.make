GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    balancer.go
    cache.go
    child_policy.go
    config.go
    control_channel.go
    picker.go
)

GO_TEST_SRCS(
    # balancer_test.go
    cache_test.go
    config_test.go
    # control_channel_test.go
    helpers_test.go
    picker_test.go
)

END()

RECURSE(
    gotest
    internal
)
