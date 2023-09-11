LIBRARY()

SRCS(
    write.cpp
    write_data.cpp
    slice_builder.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/tx/ev_write
    ydb/services/metadata
)

END()
