LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    counting_raw_reader.cpp
    job_reader.cpp
    job_writer.cpp
    lenval_table_reader.cpp
    node_table_reader.cpp
    node_table_writer.cpp
    proto_helpers.cpp
    proto_table_reader.cpp
    proto_table_writer.cpp
    skiff_row_table_reader.cpp
    skiff_table_reader.cpp
    stream_raw_reader.cpp
    yamr_table_reader.cpp
    yamr_table_writer.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/yson
    yt/cpp/mapreduce/common
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/interface/logging
    yt/yt_proto/yt/formats
    library/cpp/yson/node
    yt/cpp/mapreduce/skiff
    yt/yt/core
)

END()
