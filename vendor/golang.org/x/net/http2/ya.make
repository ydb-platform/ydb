GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    ascii.go
    ciphers.go
    client_conn_pool.go
    databuffer.go
    errors.go
    flow.go
    frame.go
    go111.go
    go115.go
    go118.go
    gotrack.go
    headermap.go
    http2.go
    pipe.go
    server.go
    transport.go
    write.go
    writesched.go
    writesched_priority.go
    writesched_random.go
    writesched_roundrobin.go
)

END()

RECURSE(
    h2c
    h2i
    hpack
)
