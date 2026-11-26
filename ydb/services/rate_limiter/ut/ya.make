UNITTEST_FOR(ydb/services/rate_limiter)

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ENDIF()

SRCS(
    rate_limiter_ut.cpp
)

PEERDIR(
    ydb/core/grpc_services
    ydb/core/testlib/default
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/coordination
    ydb/public/sdk/cpp/src/client/rate_limiter
    ydb/public/sdk/cpp/src/client/types
    ydb/public/sdk/cpp/src/library/issue
    ydb/services/ydb
)

YQL_LAST_ABI_VERSION()

END()
