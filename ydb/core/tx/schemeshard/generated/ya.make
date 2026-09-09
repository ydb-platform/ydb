LIBRARY()

PEERDIR(
    ydb/core/protos
)

RUN_PROGRAM(
    ydb/core/tx/schemeshard/generated/codegen
        dispatch_op.h.in
        dispatch_op.h
        op_type_list.h.in
        op_type_list.h
    IN dispatch_op.h.in
    IN op_type_list.h.in
    OUT dispatch_op.h
    OUT op_type_list.h
    OUTPUT_INCLUDES
        ydb/core/protos/flat_scheme_op.pb.h
)

END()

RECURSE(
    codegen
)
