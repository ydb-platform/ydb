GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    reflect.go
    util.go
    xform.go
)

GO_TEST_SRCS(
    reflect_test.go
    util_test.go
    xform_test.go
)

END()

RECURSE(gotest)
