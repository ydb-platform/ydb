GO_PROGRAM()

LICENSE(MIT)

VERSION(v1.5.1)

SRCS(
    flags.go
    main.go
)

GO_XTEST_SRCS(
    flags_test.go
    main_test.go
)

END()

RECURSE(
    gotest
)
