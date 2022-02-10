LIBRARY()

OWNER(g:passport_infra)

PEERDIR(
    library/cpp/string_utils/secret_string
    library/cpp/tvmauth/src/protos
    library/cpp/tvmauth/src/rw
)

SRCS(
    deprecated/service_context.cpp
    deprecated/user_context.cpp
    src/parser.cpp
    src/service_impl.cpp
    src/service_ticket.cpp
    src/status.cpp
    src/unittest.cpp
    src/user_impl.cpp
    src/user_ticket.cpp
    src/utils.cpp
    src/version.cpp
    utils.cpp
)

GENERATE_ENUM_SERIALIZATION(checked_user_ticket.h)
GENERATE_ENUM_SERIALIZATION(ticket_status.h)

RESOURCE(
    src/version /builtin/version
)

END()

RECURSE(
    client 
    src/rw
    src/ut
    test_all
)
