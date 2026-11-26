UNITTEST()

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

DEPENDS(ydb/apps/ydb/experimental/ydb)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/experimental/ydb/ydb")

SRCDIR(ydb/apps/ydb/ut)

SRCS(
    mock_env.cpp
    reassign_tablets.cpp
    run_ydb.cpp
)

PEERDIR(
    contrib/libs/fmt
    contrib/libs/grpc
    contrib/libs/jwt-cpp
    library/cpp/json
    ydb/core/security/certificate_check/test_utils
    ydb/public/api/client/yc_public/iam
    ydb/public/api/grpc/draft
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/draft
    ydb/public/sdk/cpp/tests/unit/client/oauth2_token_exchange/helpers
)

END()
