GO_LIBRARY()

SRCS(
    ecdsa.go
    ecdsa_legacy.go
    ecdsa_noasm.go
    notboring.go
)

GO_TEST_SRCS(ecdsa_test.go)

GO_XTEST_SRCS(
    equal_test.go
    example_test.go
)

END()

RECURSE(
)
