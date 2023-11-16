GO_LIBRARY()

LICENSE(MIT)

SRCS(
    city64.go
    cityhash.go
    doc.go
)

GO_TEST_SRCS(cityhash_test.go)

END()

RECURSE(gotest)
