LIBRARY()

PEERDIR(
    ydb/core/protos
)

RUN_PROGRAM(
    ydb/core/tx/schemeshard/generated/codegen
        dispatch_op.h.in
        dispatch_op.h
        operation_registry.h.in
        operation_registry.h
    IN dispatch_op.h.in operation_registry.h.in
    OUT dispatch_op.h operation_registry.h
    OUTPUT_INCLUDES
        ydb/core/protos/flat_scheme_op.pb.h
        ydb/core/protos/schemeshard/operations.pb.h
        util/system/yassert.h
)

END()

RECURSE(
    codegen
)
