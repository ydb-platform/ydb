LIBRARY()

PEERDIR(
    ydb/core/protos
    ydb/core/control/lib/base
    ydb/library/yverify_stream
    library/cpp/threading/hot_swap
)

RUN_PROGRAM(
    ydb/core/control/lib/generated/codegen
        control_board_proto.h.in
        control_board_proto.h
    OUTPUT_INCLUDES
        ydb/core/protos/tablet.pb.h
        ydb/core/protos/config.pb.h
    IN
        control_board_proto.h.in
    OUT
        control_board_proto.h
)

RUN_PROGRAM(
    ydb/core/control/lib/generated/codegen
        control_board_proto.cpp.in
        control_board_proto.cpp
    OUTPUT_INCLUDES
        ydb/core/protos/tablet.pb.h
        ydb/core/protos/config.pb.h
        ydb/core/control/lib/generated/control_board_proto.h
    IN
        control_board_proto.cpp.in
    OUT
        control_board_proto.cpp
)

END()

RECURSE(
    codegen
)
