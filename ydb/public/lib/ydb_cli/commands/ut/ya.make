UNITTEST_FOR(ydb/public/lib/ydb_cli/commands)

REQUIREMENTS(cpu:1)
PEERDIR(
    library/cpp/json
)

SRCS(
    oidc_credentials_ut.cpp
    completion_ut.cpp
)

END()
