UNITTEST()

SRCS(
    iam_delegation_ut.cpp
)

PEERDIR(
    ydb/core/kqp/gateway/behaviour/external_data_source
    ydb/public/api/client/yc_private/iam
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg
)

YQL_LAST_ABI_VERSION()

END()
