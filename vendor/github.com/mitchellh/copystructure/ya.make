GO_LIBRARY()

LICENSE(MIT)

VERSION(v1.2.0)

SRCS(
    copier_time.go
    copystructure.go
)

GO_TEST_SRCS(
    copier_time_test.go
    copystructure_examples_test.go
    copystructure_test.go
)

END()

RECURSE(
    gotest
)
