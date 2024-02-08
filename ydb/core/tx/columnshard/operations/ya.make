LIBRARY()

SRCS(
    write.cpp
    write_data.cpp
    slice_builder.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/tx/data_events
    ydb/services/metadata
)

END()
