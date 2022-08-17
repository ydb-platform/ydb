LIBRARY()

OWNER(
    g:kikimr
    g:logbroker
)

SRCS(
    executor.h
    executor.cpp
    read_session_event.cpp
    counters.cpp
    deferred_commit.cpp
    event_handlers.cpp
    read_session.h
    read_session.cpp
    topic_impl.h
    topic_impl.cpp
    topic.cpp
)

PEERDIR(
    library/cpp/grpc/client
    library/cpp/monlib/dynamic_counters
    library/cpp/string_utils/url
    ydb/library/persqueue/obfuscate
    ydb/public/api/grpc/draft
    ydb/public/sdk/cpp/client/impl/ydb_internal/make_request
    ydb/public/sdk/cpp/client/ydb_common_client/impl
    ydb/public/sdk/cpp/client/ydb_driver
    ydb/public/sdk/cpp/client/ydb_persqueue_core/impl

    ydb/public/api/grpc/draft
)

END()
