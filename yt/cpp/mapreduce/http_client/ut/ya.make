GTEST()

SRCS(
    raw_batch_request_ut.cpp
    raw_requests_ut.cpp
)

PEERDIR(
    yt/cpp/mapreduce/http_client
)

END()
