GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(bsontype.go)

GO_TEST_SRCS(bsontype_test.go)

END()

RECURSE(gotest)
