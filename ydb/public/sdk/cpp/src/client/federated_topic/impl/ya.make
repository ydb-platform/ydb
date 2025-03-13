LIBRARY()

SRCS(
    federated_read_session.h
    federated_read_session.cpp
    federated_read_session_event.cpp
    federated_write_session.h
    federated_write_session.cpp
    federated_topic_impl.h
    federated_topic_impl.cpp
    federated_topic.cpp
    federation_observer.h
    federation_observer.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/library/grpc/client
    library/cpp/monlib/dynamic_counters
    library/cpp/monlib/metrics
    library/cpp/string_utils/url
    ydb/public/sdk/cpp/src/library/persqueue/obfuscate
    ydb/public/api/grpc/draft
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/client/impl/ydb_internal/make_request
    ydb/public/sdk/cpp/src/client/common_client/impl
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/topic/impl
    ydb/public/sdk/cpp/src/client/proto
)

END()
