UNITTEST()

SIZE(SMALL)

SRCS(
    kqp_compile_trace_transformer_ut.cpp
    kqp_tracing_ut.cpp
)

PEERDIR(
    ydb/core/kqp/tracing
    ydb/core/kqp/tracing/test_util
    ydb/library/actors/testlib
    yql/essentials/core
)

YQL_LAST_ABI_VERSION()

END()
