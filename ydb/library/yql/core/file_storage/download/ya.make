LIBRARY()

SRCS(
    download_config.cpp
    download_stream.cpp
)

PEERDIR(
    ydb/library/yql/core/file_storage/proto
    library/cpp/protobuf/util
)

END()
