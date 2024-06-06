LIBRARY()

YQL_LAST_ABI_VERSION()

SRCS(
    yql_s3_actors_factory.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/libs/curl
    library/cpp/lwtrace/protos
    ydb/core/fq/libs/protos
    ydb/library/mkql_proto/protos
    ydb/library/yql/dq/actors/protos
    ydb/library/yql/providers/s3/proto
)

END()
