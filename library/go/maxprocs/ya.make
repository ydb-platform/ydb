GO_LIBRARY()

SRCS(
    cgroups.go
    doc.go
    helpers.go
    maxprocs.go
)

GO_XTEST_SRCS(
    example_test.go
    maxprocs_test.go
)

END()

RECURSE(
    example
    gotest
)
