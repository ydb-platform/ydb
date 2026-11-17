UNITTEST_FOR(ydb/public/lib/ydb_cli/commands)

PEERDIR(
    library/cpp/json
    ydb/public/api/grpc/draft
)

SRCS(
    bridge_list_ut.cpp
    bridge_ut.cpp
    oidc_credentials_ut.cpp
    completion_ut.cpp
)

END()
