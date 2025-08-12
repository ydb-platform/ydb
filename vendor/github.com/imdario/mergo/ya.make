GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v0.3.15)

SRCS(
    doc.go
    map.go
    merge.go
    mergo.go
)

GO_TEST_SRCS(pr211_2_test.go)

GO_XTEST_SRCS(
    issue100_test.go
    issue104_test.go
    issue121_test.go
    issue123_test.go
    issue125_test.go
    issue129_test.go
    issue131_test.go
    issue136_test.go
    issue138_test.go
    issue143_test.go
    issue149_test.go
    issue174_test.go
    issue17_test.go
    issue202_test.go
    issue209_test.go
    issue220_test.go
    issue230_test.go
    issue23_test.go
    issue33_test.go
    issue38_test.go
    issue50_test.go
    issue52_test.go
    issue61_test.go
    issue64_test.go
    issue66_test.go
    issue83_test.go
    issue84_test.go
    issue89_test.go
    issue90_test.go
    issueXXX_test.go
    merge_test.go
    mergo_test.go
    pr211_test.go
    pr80_test.go
    pr81_test.go
    v039_bugs_test.go
)

END()

RECURSE(
    gotest
)
