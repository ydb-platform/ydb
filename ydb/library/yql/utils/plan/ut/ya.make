UNITTEST_FOR(ydb/library/yql/utils/plan)

TAG(ya:manual)

SRCS(
    plan_utils_ut.cpp
)

PEERDIR(
    ydb/library/yql/ast
    ydb/library/yql/providers/common/provider
    ydb/library/yql/public/udf/service/stub
    ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
