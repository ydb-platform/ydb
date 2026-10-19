UNITTEST_FOR(ydb/public/lib/ydb_cli/commands)

PEERDIR(
    library/cpp/json
)

SRCS(
    oidc_credentials_ut.cpp
    completion_ut.cpp
    import_yt_ut.cpp
)

END()
