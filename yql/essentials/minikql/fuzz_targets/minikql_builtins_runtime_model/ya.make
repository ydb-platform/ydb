FUZZ()

SRCS(
    main.cpp
)

PEERDIR(
    contrib/libs/libfuzzer
    yql/essentials/minikql
    yql/essentials/minikql/comp_nodes/no_llvm
    yql/essentials/minikql/invoke_builtins/no_llvm
    yql/essentials/minikql/runtime_settings
    yql/essentials/public/issue/protos
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
