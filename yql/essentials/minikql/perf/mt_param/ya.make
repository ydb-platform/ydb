PROGRAM()

PEERDIR(
    yql/essentials/minikql/comp_nodes/llvm14
    yql/essentials/public/udf
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    mt_param.cpp
)

END()
