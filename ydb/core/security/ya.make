LIBRARY()

SRCS(
    login_page.cpp
    login_page.h
    login_shared_func.cpp
    secure_request.h
    ticket_parser_impl.h
    ticket_parser.cpp
    ticket_parser.h
    ldap_auth_provider.cpp
    ldap_utils.cpp
)

IF(OS_LINUX OR OS_DARWIN)
    PEERDIR(
        contrib/libs/openldap
    )

    SRCS(
        ldap_auth_provider_linux.cpp
    )
ELSEIF(OS_WINDOWS)
    EXTRALIBS_STATIC(wldap32.lib)

    SRCS(
        ldap_auth_provider_win.cpp
    )
ENDIF()

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/http
    ydb/library/grpc/actor_client
    library/cpp/monlib/service/pages
    library/cpp/openssl/io
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
)

RECURSE(
    certificate_check
)
