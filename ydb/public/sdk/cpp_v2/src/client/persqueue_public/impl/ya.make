LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

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
    library/cpp/retry
    library/cpp/string_utils/url
    library/cpp/containers/disjoint_interval_tree
    ydb/public/sdk/cpp_v2/src/library/grpc/client
    ydb/public/sdk/cpp_v2/src/library/persqueue/obfuscate
    ydb/public/api/grpc/draft
    ydb/public/sdk/cpp_v2/src/client/impl/ydb_internal/make_request
    ydb/public/sdk/cpp_v2/src/client/common_client/impl
    ydb/public/sdk/cpp_v2/src/client/driver
    ydb/public/sdk/cpp_v2/src/client/topic/codecs
    ydb/public/sdk/cpp_v2/src/client/topic/common
    ydb/public/sdk/cpp_v2/src/client/topic/impl
)

END()
