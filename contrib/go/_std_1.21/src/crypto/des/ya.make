GO_LIBRARY()

SRCS(
    block.go
    cipher.go
    const.go
)

GO_TEST_SRCS(des_test.go)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
)
