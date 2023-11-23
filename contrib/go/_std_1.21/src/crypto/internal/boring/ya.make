GO_LIBRARY()

SRCS(
    doc.go
    notboring.go
)

GO_TEST_SRCS(boring_test.go)

END()

RECURSE(
    bbig
    bcache
    sig
)
