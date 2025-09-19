GO_LIBRARY()

LICENSE(MIT)

VERSION(v1.5.0)

SRCS(
    common.go
    convert.go
    count.go
    doc.go
    format.go
    manipulate.go
    stringbuilder.go
    translate.go
)

GO_TEST_SRCS(
    convert_test.go
    count_test.go
    format_test.go
    manipulate_test.go
    translate_test.go
    util_test.go
)

END()

RECURSE(
    gotest
)
