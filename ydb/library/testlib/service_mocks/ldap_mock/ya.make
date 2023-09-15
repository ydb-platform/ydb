LIBRARY()

SRCS(
    ldap_simple_server.cpp
    ldap_message_processor.cpp
    ldap_mock.cpp
    ldap_response.cpp
    ber.cpp
    ldap_defines.cpp
    ldap_socket_wrapper.cpp
)

PEERDIR(
    contrib/libs/openssl
)

END()
