GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    city64.go
    cityhash.go
    doc.go
)

GO_TEST_SRCS(cityhash_test.go)

END()
