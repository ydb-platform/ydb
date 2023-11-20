GO_LIBRARY()

LICENSE(MIT)

GO_SKIP_TESTS(
    Test_Mock_Chained_On
    Test_Mock_Chained_UnsetOnlyUnsetsLastCall
)

SRCS(
    doc.go
    mock.go
)

GO_TEST_SRCS(mock_test.go)

END()

RECURSE(gotest)
