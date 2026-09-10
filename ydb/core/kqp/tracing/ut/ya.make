UNITTEST()

SIZE(SMALL)

SRCS(
    kqp_tracing_ut.cpp
)

PEERDIR(
    ydb/core/kqp/tracing
    ydb/core/kqp/tracing/test_util
    ydb/library/actors/testlib
    ydb/library/yql/dq/actors/compute
)

YQL_LAST_ABI_VERSION()

END()
