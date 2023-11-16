GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    alignment.go
    env.go
    errors.go
    harness.go
    text_map_carrier.go
    text_map_propagator.go
)

GO_TEST_SRCS(
    env_test.go
    text_map_carrier_test.go
    text_map_propagator_test.go
)

END()

RECURSE(gotest)
