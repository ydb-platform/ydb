LIBRARY()

SRCS(
    yql_pq_composite_read_session.cpp
)

PEERDIR(
    library/cpp/protobuf/interop
    library/cpp/threading/future
    ydb/library/accessor
    ydb/library/actors/core
    ydb/library/services
    ydb/library/signals
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/dq/common
    ydb/library/yverify_stream
    ydb/public/sdk/cpp/src/client/topic
)

END()
