GO_LIBRARY()

LICENSE(MIT)

SRCS(
    doc.go
    write.go
)

GO_TEST_SRCS(write_test.go)

END()

RECURSE(gotest)
