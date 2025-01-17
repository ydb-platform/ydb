UNITTEST_FOR(ydb/core/tx/mediator)

SIZE(MEDIUM)

PEERDIR(
    ydb/core/testlib/default
    ydb/core/tx
    ydb/core/tx/coordinator/public
    ydb/core/tx/time_cast
    ydb/public/api/grpc
)

YQL_LAST_ABI_VERSION()

SRCS(
    mediator_ut.cpp
)

END()
