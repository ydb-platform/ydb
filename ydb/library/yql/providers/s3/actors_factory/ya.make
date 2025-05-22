LIBRARY()

YQL_LAST_ABI_VERSION()

SRCS(
    yql_s3_actors_factory.cpp
)

PEERDIR(
    ydb/core/fq/libs/protos
    ydb/library/actors/core
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/dq/common
    ydb/library/yql/providers/common/token_accessor/client
    ydb/library/yql/providers/common/http_gateway
    ydb/library/yql/providers/s3/proto
)

END()
