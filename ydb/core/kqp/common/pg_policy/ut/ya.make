UNITTEST()

SRCS(
    pg_policy_ut.cpp
)

PEERDIR(
    ydb/core/kqp/common/pg_policy
    yql/essentials/core
    yql/essentials/core/services
    yql/essentials/core/type_ann
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg
)

YQL_LAST_ABI_VERSION()

END()
