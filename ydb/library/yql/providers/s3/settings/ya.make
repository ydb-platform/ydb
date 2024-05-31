LIBRARY()

PEERDIR(
    ydb/library/yql/providers/s3/expr_nodes
    ydb/library/yql/providers/s3/proto
)

SRCS(
    yql_s3_settings.cpp
)

YQL_LAST_ABI_VERSION()

END()
