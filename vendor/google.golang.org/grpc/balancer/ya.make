GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    balancer.go
    conn_state_evaluator.go
)

GO_TEST_SRCS(conn_state_evaluator_test.go)

END()

RECURSE(
    base
    gotest
    grpclb
    rls
    roundrobin
    weightedroundrobin
    weightedtarget
)
