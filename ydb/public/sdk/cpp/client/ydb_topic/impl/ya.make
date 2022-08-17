LIBRARY()

OWNER(
    g:kikimr
    g:logbroker
)

SRCS(
    topic_impl.h
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

    ydb/public/api/grpc/draft
)

END()
