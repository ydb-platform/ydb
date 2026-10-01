LIBRARY()

PEERDIR(
    ydb/core/protos
    ydb/library/actors/core
    ydb/library/protobuf_printer
)

SRCS(
    family_settings.cpp
    family_settings.h
    sensitive_event_pb.h
    worker_id.cpp
)

YQL_LAST_ABI_VERSION()

END()
