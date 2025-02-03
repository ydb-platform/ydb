GO_PROGRAM()

LICENSE(BSD-3-Clause)

VERSION(v1.5.4)

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
