PROTO_LIBRARY()

SRCS(
    portion_info.proto
)

PEERDIR(
    ydb/library/formats/arrow/protos
    ydb/core/tx/columnshard/common/protos

)

END()
