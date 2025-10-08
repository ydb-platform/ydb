LIBRARY()

ENABLE(YQL_STYLE_CPP)

SRCS(
    download_config.cpp
    download_stream.cpp
)

PEERDIR(
    yql/essentials/core/file_storage/proto
    library/cpp/protobuf/util
)

END()
