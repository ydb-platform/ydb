LIBRARY()

SRCS(
    ranges.cpp
    ranges_stream_drain.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/query
    ydb/public/sdk/cpp/src/client/result
    ydb/public/sdk/cpp/src/client/table
)

END()
