UNITTEST()

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

DEPENDS(ydb/apps/ydb)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")

SRCDIR(ydb/apps/ydb/ut)

SRCS(
    mock_env.cpp
    oidc.cpp
    parse_command_line.cpp
    run_ydb.cpp
)

PEERDIR(
    contrib/libs/fmt
    contrib/libs/grpc
    contrib/libs/jwt-cpp
    library/cpp/json/writer
    ydb/core/security/certificate_check/test_utils
    ydb/public/api/client/yc_public/iam
    ydb/public/sdk/cpp/tests/unit/client/oauth2_token_exchange/helpers
    ydb/public/sdk/cpp/tests/unit/client/oidc/helpers
)

END()
