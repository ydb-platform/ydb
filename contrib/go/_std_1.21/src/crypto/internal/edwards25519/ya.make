GO_LIBRARY()

SRCS(
    doc.go
    edwards25519.go
    scalar.go
    scalar_fiat.go
    scalarmult.go
    tables.go
)

GO_TEST_SRCS(
    edwards25519_test.go
    scalar_alias_test.go
    scalar_test.go
    scalarmult_test.go
    tables_test.go
)

END()

RECURSE(
    field
)
