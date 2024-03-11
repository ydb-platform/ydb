UNITTEST_FOR(ydb/core/client/metadata)

SRCS(
    functions_metadata_ut.cpp
)

PEERDIR(
    ydb/library/yql/minikql/invoke_builtins/llvm14
    ydb/library/yql/public/udf/service/stub
    ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
