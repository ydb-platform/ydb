LIBRARY()

PEERDIR(
    library/cpp/json
    ydb/core/protos
    ydb/library/actors/core
    ydb/library/protobuf_printer
)

SRCS(
    changefeed.cpp
    family_settings.cpp
    family_settings.h
    schema_change.cpp
    schema_change.h
    sensitive_event_pb.h
    worker_id.cpp
)

END()
