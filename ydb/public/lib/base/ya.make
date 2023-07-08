LIBRARY()

SRCS(
    defs.h
    msgbus_status.h
    msgbus.h
    msgbus.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/deprecated/enum_codegen
    library/cpp/messagebus
    library/cpp/messagebus/protobuf
    ydb/core/protos
)

END()
