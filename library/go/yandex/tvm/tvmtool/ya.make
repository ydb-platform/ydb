GO_LIBRARY()

SRCS(
    any.go
    deploy.go
    doc.go
    errors.go
    opts.go
    qloud.go
    tool.go
)

GO_TEST_SRCS(tool_export_test.go)

GO_XTEST_SRCS(
    any_example_test.go
    deploy_example_test.go
    qloud_example_test.go
    tool_bg_update_test.go
    tool_example_test.go
)

IF (OS_LINUX)
    GO_XTEST_SRCS(
        clients_test.go
        tool_test.go
    )
ENDIF()

IF (OS_DARWIN)
    GO_XTEST_SRCS(
        clients_test.go
        tool_test.go
    )
ENDIF()

END()

RECURSE(
    examples
    internal
)

RECURSE_FOR_TESTS(gotest)
