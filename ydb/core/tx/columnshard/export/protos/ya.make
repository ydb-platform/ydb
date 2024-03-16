PROTO_LIBRARY()

SRCS(
    cursor.proto
    selector.proto
    task.proto
)

PEERDIR(
    ydb/core/tx/columnshard/common/protos
)

END()
