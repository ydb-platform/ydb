PROTO_LIBRARY()

SRCS(
    cursor.proto
    selector.proto
    storage.proto
    task.proto
)

PEERDIR(
    ydb/core/tx/columnshard/common/protos
    ydb/core/protos
)

END()
