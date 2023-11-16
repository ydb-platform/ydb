GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    go118.go
    idna10.0.0.go
    punycode.go
    tables13.0.0.go
    trie.go
    trie13.0.0.go
    trieval.go
)

GO_TEST_SRCS(
    common_test.go
    conformance_test.go
    gen10.0.0_test.go
    idna10.0.0_test.go
    idna_test.go
    punycode_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(gotest)
