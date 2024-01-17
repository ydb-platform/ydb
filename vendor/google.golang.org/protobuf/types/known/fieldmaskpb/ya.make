GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(field_mask.pb.go)

GO_XTEST_SRCS(field_mask_test.go)

END()

RECURSE(gotest)
