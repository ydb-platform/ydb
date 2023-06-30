LIBRARY()

SRCS(
    res_or_pull.cpp
    table_limiter.cpp
)

PEERDIR(
    library/cpp/yson
    ydb/library/yql/minikql
    ydb/library/yql/public/udf
    ydb/library/yql/utils
    ydb/library/yql/providers/common/codec
    ydb/library/yql/providers/yt/codec
    ydb/library/yql/providers/yt/lib/mkql_helpers
)

YQL_LAST_ABI_VERSION()

END()
