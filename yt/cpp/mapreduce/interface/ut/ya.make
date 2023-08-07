UNITTEST_FOR(yt/cpp/mapreduce/interface)

SRCS(
    common_ut.cpp
    config_ut.cpp
    error_ut.cpp
    format_ut.cpp
    job_counters_ut.cpp
    job_statistics_ut.cpp
    operation_ut.cpp
    proto3_ut.proto
    protobuf_table_schema_ut.cpp
    protobuf_file_options_ut.cpp
    protobuf_table_schema_ut.proto
    protobuf_file_options_ut.proto
    serialize_ut.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/testing/unittest
    yt/yt_proto/yt/formats
)

END()
