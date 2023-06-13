LIBRARY()

SRCS(
    login_page.cpp
    login_page.h
    secure_request.h
    ticket_parser_impl.h
    ticket_parser.cpp
    ticket_parser.h
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/http
    library/cpp/monlib/service/pages
    library/cpp/openssl/io
    ydb/core/base
    ydb/core/protos
    ydb/library/ycloud/api
    ydb/library/ycloud/impl
    ydb/library/aclib
    ydb/library/aclib/protos
    ydb/library/login
    ydb/library/security
)

END()

RECURSE_FOR_TESTS(
    ut
)
