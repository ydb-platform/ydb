PROTO_LIBRARY(library-formats-arrow-protos)
PROTOC_FATAL_WARNINGS()

SRCS(
    ssa.proto
    fields.proto
    accessor.proto
)

PEERDIR(

)

END()
