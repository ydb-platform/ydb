GO_LIBRARY()

LICENSE(
    Apache-2.0 AND
    BSD-2-Clause AND
    BSD-3-Clause AND
    BSL-1.0 AND
    CC-BY-3.0 AND
    HPND AND
    MIT AND
    NCSA AND
    OpenSSL AND
    Zlib
)

SRCS(
    client.go
    client_auth.go
    doc.go
    gen.go
    record_batch_reader.go
    record_batch_writer.go
    server.go
    server_auth.go
)

GO_XTEST_SRCS(
    basic_auth_flight_test.go
    example_flight_server_test.go
    flight_middleware_test.go
    flight_test.go
    server_example_test.go
)

END()

RECURSE(
    flightsql
    internal
)
