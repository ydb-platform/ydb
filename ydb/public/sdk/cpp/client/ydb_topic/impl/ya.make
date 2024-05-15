LIBRARY()

SRCS(
    common.h
    common.cpp
    counters_logger.h
    deferred_commit.cpp
    event_handlers.cpp
    read_session_event.cpp
    read_session_impl.ipp
    read_session.h
    read_session.cpp
    topic_impl.h
    topic_impl.cpp
    topic.cpp
    write_session_impl.h
    write_session_impl.cpp
    write_session.h
    write_session.cpp
)

PEERDIR(
    ydb/library/grpc/client
    library/cpp/monlib/dynamic_counters
    library/cpp/monlib/metrics
    library/cpp/string_utils/url
    ydb/library/persqueue/obfuscate
    ydb/public/api/grpc/draft
    ydb/public/api/grpc
    ydb/public/sdk/cpp/client/impl/ydb_internal/make_request
    ydb/public/sdk/cpp/client/ydb_common_client/impl
    ydb/public/sdk/cpp/client/ydb_driver
    ydb/public/sdk/cpp/client/ydb_topic/common
    ydb/public/sdk/cpp/client/ydb_topic/include
    ydb/public/sdk/cpp/client/ydb_proto
)

END()
