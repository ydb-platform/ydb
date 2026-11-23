UNITTEST()

SRCS(kqp_eval_ut.cpp)

PEERDIR(
    ydb/core/kqp/opt/rbo/eval
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

END()
