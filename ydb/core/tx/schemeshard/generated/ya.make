LIBRARY()

PEERDIR(
    ydb/core/protos
)

RUN_PROGRAM(
    ydb/core/tx/schemeshard/generated/codegen
        dispatch_op.h.in
        dispatch_op.h
        operation_registry_checks.inc.in
        operation_registry_checks.inc
    IN dispatch_op.h.in operation_registry_checks.inc.in
    OUT dispatch_op.h operation_registry_checks.inc
    OUTPUT_INCLUDES
        ydb/core/protos/flat_scheme_op.pb.h
)

END()

RECURSE(
    codegen
)
