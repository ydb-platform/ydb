UNITTEST_FOR(ydb/library/yql/providers/dq/scheduler)

TAG(ya:manual)

SIZE(SMALL)

SRCS(
    dq_scheduler_ut.cpp
)

PEERDIR(
    ydb/library/yql/public/udf/service/stub
    ydb/library/yql/utils/log
)

YQL_LAST_ABI_VERSION()

END()
