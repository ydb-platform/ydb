GO_LIBRARY()

SRCS(
    alert.go
    auth.go
    cache.go
    cipher_suites.go
    common.go
    common_string.go
    conn.go
    handshake_client.go
    handshake_client_tls13.go
    handshake_messages.go
    handshake_server.go
    handshake_server_tls13.go
    key_agreement.go
    key_schedule.go
    notboring.go
    prf.go
    quic.go
    ticket.go
    tls.go
)

GO_TEST_SRCS(
    auth_test.go
    cache_test.go
    conn_test.go
    handshake_client_test.go
    handshake_messages_test.go
    handshake_server_test.go
    handshake_test.go
    key_schedule_test.go
    link_test.go
    prf_test.go
    quic_test.go
    ticket_test.go
    tls_test.go
)

GO_XTEST_SRCS(example_test.go)

IF (OS_LINUX)
    GO_TEST_SRCS(handshake_unix_test.go)
ENDIF()

IF (OS_DARWIN)
    GO_TEST_SRCS(handshake_unix_test.go)
ENDIF()

END()

RECURSE(
)
