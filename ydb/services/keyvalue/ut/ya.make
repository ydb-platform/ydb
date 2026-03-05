UNITTEST_FOR(ydb/services/keyvalue)

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ENDIF()

SRCS(
    grpc_service_ut.cpp
)

PEERDIR(
    library/cpp/logger
    ydb/core/protos
    ydb/core/testlib/default
    ydb/services/keyvalue
)

YQL_LAST_ABI_VERSION()

END()
