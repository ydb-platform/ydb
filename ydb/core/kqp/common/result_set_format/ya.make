LIBRARY()

SRCS(
    kqp_result_set_arrow.cpp
    kqp_result_set_builders.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/public/api/protos
    ydb/library/mkql_proto/protos
    ydb/library/yql/dq/proto
    ydb/library/yql/dq/runtime
    yql/essentials/minikql
    yql/essentials/public/udf
)

YQL_LAST_ABI_VERSION()

END()
