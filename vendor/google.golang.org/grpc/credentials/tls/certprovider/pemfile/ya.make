GO_LIBRARY()

LICENSE(Apache-2.0)

GO_SKIP_TESTS(
    Test
    Provider_NoUpdate
    Provider_UpdateFailure_ThenSuccess
    Provider_UpdateSuccess
    Provider_UpdateSuccessWithSymlink
)

SRCS(
    builder.go
    watcher.go
)

GO_TEST_SRCS(
    builder_test.go
    watcher_test.go
)

END()

RECURSE(gotest)
