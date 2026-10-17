UNITTEST_FOR(yql/essentials/core/file_storage/download)

SRCS(
    download_limiter_ut.cpp
    download_output_file_stream_ut.cpp
)

PEERDIR(
    library/cpp/threading/future
)

END()
