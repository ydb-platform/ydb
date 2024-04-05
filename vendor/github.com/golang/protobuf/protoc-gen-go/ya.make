GO_PROGRAM()

LICENSE(BSD-3-Clause)

SRCS(
    main.go
)

END()

RECURSE(
    descriptor
    generator
    grpc
    plugin
)
