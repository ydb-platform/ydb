LIBRARY()

SRCS(
    download_config.cpp
    download_limiter.cpp
    download_output_file_stream.cpp
    download_stream.cpp
)

PEERDIR(
    library/cpp/protobuf/util
    library/cpp/streams/special
    library/cpp/string_utils/parse_size
    yql/essentials/core/file_storage/proto
    yql/essentials/utils
)

END()

RECURSE_FOR_TESTS(
    ut
)
