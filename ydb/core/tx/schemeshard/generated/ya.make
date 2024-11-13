LIBRARY()

PEERDIR(
    ydb/core/protos
)

RUN_PROGRAM(
    ydb/core/tx/schemeshard/generated/codegen
        dispatch_op.h.in
        dispatch_op.h
    IN dispatch_op.h.in
    OUT dispatch_op.h
    OUTPUT_INCLUDES
        ydb/core/tx/schemeshard/generated/traits.h
)

SRCS(
    traits.h
)

END()

RECURSE(
    codegen
)
