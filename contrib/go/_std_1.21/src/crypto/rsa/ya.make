GO_LIBRARY()

SRCS(
    notboring.go
    pkcs1v15.go
    pss.go
    rsa.go
)

GO_TEST_SRCS(rsa_export_test.go)

GO_XTEST_SRCS(
    equal_test.go
    example_test.go
    pkcs1v15_test.go
    pss_test.go
    rsa_test.go
)

END()

RECURSE(
)
