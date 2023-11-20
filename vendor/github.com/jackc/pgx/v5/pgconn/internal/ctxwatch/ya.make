GO_LIBRARY()

LICENSE(MIT)

SRCS(context_watcher.go)

GO_XTEST_SRCS(context_watcher_test.go)

END()

RECURSE(gotest)
