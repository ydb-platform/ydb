GO_LIBRARY()

LICENSE(BSD-3-Clause)

GO_SKIP_TESTS(
    TestRetrieveOutputFileSubjectTokenNotJSON
    TestRetrieveOutputFileSubjectTokenFailureTests
    TestRetrieveOutputFileSubjectTokenInvalidCache
    TestRetrieveOutputFileSubjectTokenJwt
)

SRCS(
    aws.go
    basecredentials.go
    err.go
    executablecredsource.go
    filecredsource.go
    header.go
    impersonate.go
    urlcredsource.go
)

GO_TEST_SRCS(
    aws_test.go
    basecredentials_test.go
    err_test.go
    executablecredsource_test.go
    filecredsource_test.go
    header_test.go
    impersonate_test.go
    urlcredsource_test.go
)

END()

RECURSE(
    gotest
)
