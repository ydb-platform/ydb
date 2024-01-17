GO_LIBRARY()

SRCS(
    ar.go
    file.go
    xcoff.go
)

GO_TEST_SRCS(
    ar_test.go
    file_test.go
)

END()

RECURSE(
)
