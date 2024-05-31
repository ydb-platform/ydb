LIBRARY()

PEERDIR(
    ydb/library/yql/providers/common/dq
    ydb/library/yql/providers/common/http_gateway
    ydb/library/yql/dq/actors/protos
    ydb/core/fq/libs/protos
    ydb/library/mkql_proto/protos
    ydb/library/yql/providers/s3/proto
    ydb/library/yql/providers/s3/settings
)

SRCS(
    s3_dummy.cpp
)

YQL_LAST_ABI_VERSION()

END()
