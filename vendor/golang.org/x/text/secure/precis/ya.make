GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    class.go
    context.go
    doc.go
    nickname.go
    options.go
    profile.go
    profiles.go
    tables13.0.0.go
    transformer.go
    trieval.go
)

GO_TEST_SRCS(
    benchmark_test.go
    class_test.go
    enforce10.0.0_test.go
    enforce_test.go
    profile_test.go
    tables_test.go
)

END()

RECURSE(gotest)
