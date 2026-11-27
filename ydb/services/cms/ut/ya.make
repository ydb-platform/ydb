UNITTEST_FOR(ydb/services/cms)

IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    cms_ut.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/testlib/default
    ydb/library/aclib
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/params
    ydb/public/sdk/cpp/src/client/resources
    ydb/public/sdk/cpp/src/client/result
    ydb/public/sdk/cpp/src/library/grpc/client
    ydb/services/ydb
    yql/essentials/public/issue
)

YQL_LAST_ABI_VERSION()

END()
