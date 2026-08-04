FUZZ()

SIZE(LARGE)

TAG(
    ya:fat
)

SRCS(
    main.cpp
    ../../../kqp/common/kqp_types.cpp
)

PEERDIR(
    ydb/core/engine
    yql/essentials/minikql/comp_nodes
    yql/essentials/minikql/invoke_builtins
    yql/essentials/minikql/invoke_builtins/no_llvm
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
