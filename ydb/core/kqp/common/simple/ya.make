LIBRARY()

SRCS(
    helpers.cpp
    query_id.cpp
    settings.cpp
    services.cpp
    kqp_event_ids.cpp
    temp_tables.cpp
)

PEERDIR(
    contrib/libs/protobuf
    ydb/core/base
    ydb/core/protos
    ydb/library/yql/ast
    ydb/library/yql/dq/actors
    ydb/public/api/protos
)

END()
