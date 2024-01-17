GO_LIBRARY()

SRCS(
    match.go
    path.go
)

GO_XTEST_SRCS(
    example_test.go
    match_test.go
    path_test.go
)

END()

RECURSE(
    filepath
)
