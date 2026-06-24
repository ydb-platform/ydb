LIBRARY()

PEERDIR(
    ydb/core/protos
)

RUN_PROGRAM(
    ydb/core/tx/schemeshard/generated/codegen
        --yaml=op_handler_overrides.yaml
        dispatch_op.h.in
        dispatch_op.h
        op_handlers.h.in
        op_handlers.h
    IN dispatch_op.h.in
    IN op_handlers.h.in
    IN op_handler_overrides.yaml
    OUT dispatch_op.h
    OUT op_handlers.h
    OUTPUT_INCLUDES
        ydb/core/protos/flat_scheme_op.pb.h
        ydb/core/tx/schemeshard/schemeshard__operation_part.h
)

END()

RECURSE(
    codegen
)
