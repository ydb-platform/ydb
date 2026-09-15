UNITTEST_FOR(ydb/core/fq/libs/state)

SRCS(
    dq_state_load_plan_ut.cpp
)

PEERDIR(
    ydb/library/yql/providers/dq/api/protos
)

YQL_LAST_ABI_VERSION()

END()
