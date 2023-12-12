GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    metadata.go
    middleware.go
    osname.go
    recursion_detection.go
    request_id.go
    request_id_retriever.go
    user_agent.go
)

GO_TEST_SRCS(
    metadata_test.go
    recursion_detection_test.go
    user_agent_test.go
)

GO_XTEST_SRCS(middleware_test.go)

END()

RECURSE(
    gotest
)
