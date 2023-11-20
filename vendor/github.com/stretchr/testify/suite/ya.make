GO_LIBRARY()

LICENSE(MIT)

GO_SKIP_TESTS(
    TestSuiteRequireTwice
    TestFailfastSuiteFailFastOn
    TestFailfastSuite
    TestSuiteRecoverPanic
)

SRCS(
    doc.go
    interfaces.go
    stats.go
    suite.go
)

GO_TEST_SRCS(
    stats_test.go
    suite_test.go
)

END()

RECURSE(gotest)
