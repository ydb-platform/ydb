UNITTEST()

SRCS(
    predictor_ut.cpp
)

PEERDIR(
    ydb/core/kqp/predictor
    yql/essentials/ast
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
