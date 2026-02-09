LIBRARY()

SRCS(
    yql_pq_composite_read_session.cpp
)

PEERDIR(
    ydb/library/yverify_stream
    ydb/public/sdk/cpp/src/client/topic
)

END()
