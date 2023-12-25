LIBRARY()

SRCS(
    lambda_builder.cpp
)

PEERDIR(
    library/cpp/random_provider
    library/cpp/time_provider
    ydb/library/yql/ast
    ydb/library/yql/minikql/computation
    ydb/library/yql/public/udf
    ydb/library/yql/utils
    ydb/library/yql/providers/common/mkql
)

YQL_LAST_ABI_VERSION()

END()
