GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    ext.pb.go
    test.pb.go
    test_import.pb.go
    test_public.pb.go
)

END()

RECURSE(
    weak1
    weak2
)
