LIBRARY()

SRCS(
    yql_pq_composite_read_session.cpp
)

PEERDIR(
    library/cpp/protobuf/interop
    ydb/library/actors/core
    ydb/library/services
    ydb/library/yql/dq/actors/compute
    ydb/library/yverify_stream
    ydb/public/sdk/cpp/src/client/topic
)

END()
