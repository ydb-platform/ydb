UNITTEST_FOR(ydb/services/keyvalue)

SIZE(MEDIUM)
REQUIREMENTS(cpu:2)

SRCS(
    grpc_service_ut.cpp
)

PEERDIR(
    library/cpp/logger
    library/cpp/threading/future
    ydb/core/protos
    ydb/core/testlib/default
    ydb/services/keyvalue
)

YQL_LAST_ABI_VERSION()

END()
