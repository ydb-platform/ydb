LIBRARY()

SRCS(
    aliases.h
    common.h
    common.cpp
    persqueue_impl.h
    persqueue_impl.cpp
    persqueue.cpp
    read_session.h
    read_session.cpp
    read_session_messages.cpp
    write_session_impl.h
    write_session_impl.cpp
    write_session.h
    write_session.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    library/cpp/monlib/metrics
    library/cpp/string_utils/url
    library/cpp/containers/disjoint_interval_tree
    ydb/library/grpc/client
    ydb/library/persqueue/obfuscate
    ydb/public/api/grpc/draft
    ydb/public/sdk/cpp/client/impl/ydb_internal/make_request
    ydb/public/sdk/cpp/client/ydb_common_client/impl
    ydb/public/sdk/cpp/client/ydb_driver
    ydb/public/sdk/cpp/client/ydb_topic/codecs
    ydb/public/sdk/cpp/client/ydb_topic/common
    ydb/public/sdk/cpp/client/ydb_topic/impl
)

END()
