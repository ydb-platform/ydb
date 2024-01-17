GO_LIBRARY()

SRCS(
    supported.go
    zosarch.go
)

GO_XTEST_SRCS(zosarch_test.go)

END()

RECURSE(
)
