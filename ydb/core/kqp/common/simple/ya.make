LIBRARY()

SRCS(
    helpers.cpp
    kqp_event_ids.cpp
    query_id.cpp
    reattach.cpp
    services.cpp
    session_id.cpp
    settings.cpp
    temp_tables.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/cgiparam
    library/cpp/string_utils/base64
    library/cpp/uri
    ydb/core/base
    ydb/core/protos
    yql/essentials/ast
    ydb/library/yql/dq/actors
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/library/operation_id/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
