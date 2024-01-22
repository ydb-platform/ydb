UNITTEST_FOR(ydb/library/yql/dq/type_ann)

SIZE(SMALL)

PEERDIR(
    ydb/library/yql/sql/pg_dummy
    ydb/library/yql/public/udf/service/stub
)

SRCS(dq_type_ann_ut.cpp)

YQL_LAST_ABI_VERSION()

END()
