LIBRARY()

SRCS(
    yql_simple_arrow_resolver.cpp
)

PEERDIR(
    ydb/library/yql/minikql/arrow
    ydb/library/yql/public/udf
    ydb/library/yql/providers/common/mkql
)

YQL_LAST_ABI_VERSION()

END()
