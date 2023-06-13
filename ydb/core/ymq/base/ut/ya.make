UNITTEST()

PEERDIR(
    ydb/core/base
    ydb/core/ymq/base
)

SRCS(
    action_ut.cpp
    counters_ut.cpp
    dlq_helpers_ut.cpp
    helpers_ut.cpp
    secure_protobuf_printer_ut.cpp
    queue_attributes_ut.cpp
)

END()
