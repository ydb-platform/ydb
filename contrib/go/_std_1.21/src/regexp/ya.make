GO_LIBRARY()

SRCS(
    backtrack.go
    exec.go
    onepass.go
    regexp.go
)

GO_TEST_SRCS(
    all_test.go
    exec2_test.go
    exec_test.go
    find_test.go
    onepass_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
    syntax
)
