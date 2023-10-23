UNITTEST_FOR(ydb/services/keyvalue)

SIZE(MEDIUM)

TIMEOUT(100)

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
