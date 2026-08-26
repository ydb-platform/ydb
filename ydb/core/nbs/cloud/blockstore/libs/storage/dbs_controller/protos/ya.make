PROTO_LIBRARY()

EXCLUDE_TAGS(
    GO_PROTO
    JAVA_PROTO
)

SRCS(
    dbs_controller.proto
    dbs_controller_db.proto
)

PEERDIR(
    ydb/core/nbs/cloud/storage/core/protos
    ydb/core/protos
)

END()
