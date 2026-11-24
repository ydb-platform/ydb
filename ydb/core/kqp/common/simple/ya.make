YQL_LIBRARY()

SRCS(
    current_query_stats.h
    helpers.cpp
    kqp_event_ids.cpp
    mvcc_version.h
    query_id.cpp
    reattach.cpp
    services.cpp
    session_id.cpp
    settings.cpp
    temp_tables.cpp
    tx_request.h
    user_request_context.cpp
    user_request_context.h
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/cgiparam
    library/cpp/string_utils/base64
    library/cpp/uri
    ydb/core/base
    ydb/core/protos
    ydb/core/resource_pools
    ydb/library/actors/core
    ydb/library/yql/dq/actors
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/library/operation_id/protos
    yql/essentials/ast
)

END()

RECURSE_FOR_TESTS(
    ut
)
