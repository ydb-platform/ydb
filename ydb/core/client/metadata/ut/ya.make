UNITTEST_FOR(ydb/core/client/metadata)

SRCS(
    functions_metadata_ut.cpp
)

PEERDIR(
    yql/essentials/minikql/invoke_builtins/llvm14
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
