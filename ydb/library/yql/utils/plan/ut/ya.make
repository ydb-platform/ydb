UNITTEST_FOR(ydb/library/yql/utils/plan)

REQUIREMENTS(cpu:1)
SRCS(
    plan_utils_ut.cpp
)

PEERDIR(
    yql/essentials/ast
    yql/essentials/providers/common/provider
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
