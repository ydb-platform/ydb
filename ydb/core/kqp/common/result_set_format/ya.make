YQL_LIBRARY()

SRCS(
    kqp_result_set_builders.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/formats/arrow
    ydb/core/kqp/common/result_set_format/settings
    ydb/library/mkql_proto/protos
    ydb/library/yql/dq/proto
    ydb/library/yql/dq/runtime
    ydb/public/api/protos
    yql/essentials/minikql
    yql/essentials/public/udf
)

END()

RECURSE(
    settings
)
