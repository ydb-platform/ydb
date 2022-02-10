PROTO_LIBRARY(api-grpc-draft) 

MAVEN_GROUP_ID(com.yandex.ydb) 
 
GRPC()

OWNER(
    dcherednik
    fomichev
    vvvv
    g:kikimr
)

SRCS(
    dummy.proto
    ydb_clickhouse_internal_v1.proto
    ydb_persqueue_v1.proto
    ydb_datastreams_v1.proto
    ydb_experimental_v1.proto
    ydb_s3_internal_v1.proto
    ydb_long_tx_v1.proto
    ydb_logstore_v1.proto
    yql_db_v1.proto
)

PEERDIR(
    ydb/public/api/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
