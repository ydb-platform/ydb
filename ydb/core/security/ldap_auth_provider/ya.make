LIBRARY()

SRCS(
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
    ydb/core/base
    ydb/core/protos
    ydb/core/util
)

END()

RECURSE_FOR_TESTS(
    ut
)
