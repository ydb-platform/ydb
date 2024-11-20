UNITTEST_FOR(ydb/library/yql/providers/dq/scheduler)

SIZE(SMALL)

SRCS(
    dq_scheduler_ut.cpp
)

PEERDIR(
    yql/essentials/public/udf/service/stub
    yql/essentials/utils/log
)

YQL_LAST_ABI_VERSION()

END()
