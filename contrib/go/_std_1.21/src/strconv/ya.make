GO_LIBRARY()

SRCS(
    atob.go
    atoc.go
    atof.go
    atoi.go
    bytealg.go
    ctoa.go
    decimal.go
    doc.go
    eisel_lemire.go
    ftoa.go
    ftoaryu.go
    isprint.go
    itoa.go
    quote.go
)

GO_TEST_SRCS(
    export_test.go
    internal_test.go
)

GO_XTEST_SRCS(
    atob_test.go
    atoc_test.go
    atof_test.go
    atoi_test.go
    ctoa_test.go
    decimal_test.go
    example_test.go
    fp_test.go
    ftoa_test.go
    ftoaryu_test.go
    itoa_test.go
    quote_test.go
    strconv_test.go
)

END()

RECURSE(
)
