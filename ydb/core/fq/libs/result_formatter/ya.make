LIBRARY()

SRCS(
    result_formatter.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/json/yson
    ydb/library/mkql_proto
    ydb/library/yql/ast
    ydb/library/yql/minikql/computation
    ydb/library/yql/public/udf
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/proto
    ydb/public/sdk/cpp/src/client/result
    ydb/public/sdk/cpp/src/client/value
    ydb/library/yql/providers/common/codec
    ydb/library/yql/providers/common/schema/expr
    ydb/library/yql/providers/common/schema/mkql
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
