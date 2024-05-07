PROTO_LIBRARY()

SRCS(
    portion_info.proto
)

PEERDIR(
    ydb/core/tx/columnshard/engines/scheme/statistics/protos
    ydb/core/formats/arrow/protos

)

END()
