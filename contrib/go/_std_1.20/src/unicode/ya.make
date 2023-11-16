GO_LIBRARY()

SRCS(
    casetables.go
    digit.go
    graphic.go
    letter.go
    tables.go
)

END()

RECURSE(
    utf16
    utf8
)
