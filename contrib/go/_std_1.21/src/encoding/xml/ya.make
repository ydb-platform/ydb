GO_LIBRARY()

SRCS(
    marshal.go
    read.go
    typeinfo.go
    xml.go
)

GO_TEST_SRCS(
    atom_test.go
    marshal_test.go
    read_test.go
    xml_test.go
)

GO_XTEST_SRCS(
    example_marshaling_test.go
    example_test.go
    example_text_marshaling_test.go
)

END()

RECURSE(
)
