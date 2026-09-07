LIBRARY()

SRCS(
    trace_reader.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/storage/core/compat/protos
    ydb/core/nbs/cloud/storage/core/libs/common
    ydb/core/nbs/cloud/storage/core/libs/diagnostics

    library/cpp/containers/ring_buffer
    library/cpp/json/writer
    library/cpp/lwtrace
    library/cpp/protobuf/util
)

END()
