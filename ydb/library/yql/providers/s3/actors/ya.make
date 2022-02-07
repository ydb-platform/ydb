LIBRARY()

OWNER(
    g:yq
    g:yql
)

SRCS(
    yql_s3_read_actor.cpp
    yql_s3_source_factory.cpp
)

PEERDIR(
    ydb/library/yql/minikql/computation
    ydb/library/yql/providers/common/token_accessor/client
    ydb/library/yql/public/types
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/providers/common/http_gateway
    ydb/library/yql/providers/s3/proto
)

YQL_LAST_ABI_VERSION()

END()
