YQL_LIBRARY()

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
    ydb/library/yql/dq/common
    ydb/library/yql/providers/abstract
    ydb/library/yql/providers/pq/common
    ydb/library/yql/providers/pq/gateway/abstract
    ydb/library/yverify_stream
)

END()
