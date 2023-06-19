GO_PROGRAM()

SRCS(
    main.go
)

GO_TEST_SRCS(
    main_test.go
)

END()

RECURSE_FOR_TESTS(
    gotest
)

