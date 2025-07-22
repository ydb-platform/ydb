LIBRARY()

SRCS(
    login_page.cpp
    login_page.h
    login_shared_func.cpp
    secure_request.h
    ticket_parser_impl.h
    ticket_parser.cpp
    ticket_parser.h
    xds_bootstrap_config_builder.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/http
    ydb/library/grpc/actor_client
    library/cpp/monlib/service/pages
    library/cpp/openssl/io
    ydb/core/audit
    ydb/core/base
    ydb/core/protos
    ydb/library/aclib
    ydb/library/aclib/protos
    ydb/library/login
    ydb/library/ncloud/impl
    ydb/library/security
    ydb/library/ycloud/api
    ydb/library/ycloud/impl
)

END()

RECURSE_FOR_TESTS(
    ut
    ut_xds_bootstrap_config_builder
)

RECURSE(
    certificate_check
    ldap_auth_provider
)
