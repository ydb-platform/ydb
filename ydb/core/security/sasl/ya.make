LIBRARY()

PEERDIR(
    contrib/libs/openssl
    library/cpp/digest/argonish
    library/cpp/json
    library/cpp/string_utils/base64
    ydb/core/protos
    ydb/core/tx/scheme_cache
    ydb/library/actors/core
    ydb/library/login/hashes_checker
    ydb/library/login/password_checker
    ydb/library/login/protos
    ydb/library/login/sasl
    ydb/library/ydb_issue/proto
    yql/essentials/public/issue
)

SRCS(
    base_auth_actors.cpp
    hasher.cpp
    plain_auth_actor.cpp
    plain_ldap_auth_proxy_actor.cpp
    scram_auth_actor.cpp
    static_credentials_provider.cpp
)

END()
