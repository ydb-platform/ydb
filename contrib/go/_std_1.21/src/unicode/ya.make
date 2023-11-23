GO_LIBRARY()

SRCS(
    casetables.go
    digit.go
    graphic.go
    letter.go
    tables.go
)

GO_XTEST_SRCS(
    digit_test.go
    example_test.go
    graphic_test.go
    letter_test.go
    script_test.go
)

END()

RECURSE(
    utf16
    utf8
)
