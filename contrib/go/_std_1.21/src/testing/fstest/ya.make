GO_LIBRARY()

SRCS(
    mapfs.go
    testfs.go
)

GO_TEST_SRCS(
    mapfs_test.go
    testfs_test.go
)

END()

RECURSE(
)
