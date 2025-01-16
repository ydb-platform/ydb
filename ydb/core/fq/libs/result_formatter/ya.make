LIBRARY()

SRCS(
    result_formatter.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/json/yson
    ydb/library/mkql_proto
    yql/essentials/ast
    yql/essentials/minikql/computation
    yql/essentials/public/udf
    ydb/public/api/protos
    ydb/public/sdk/cpp/client/ydb_proto
    ydb/public/sdk/cpp/client/ydb_result
    ydb/public/sdk/cpp/client/ydb_value
    yql/essentials/providers/common/codec
    yql/essentials/providers/common/schema/expr
    yql/essentials/providers/common/schema/mkql
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
