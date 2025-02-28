LIBRARY()

SRCS(
    helpers.cpp
    kqp_event_ids.cpp
    query_id.cpp
    reattach.cpp
    services.cpp
    settings.cpp
    temp_tables.cpp
)

PEERDIR(
    contrib/libs/protobuf
    ydb/core/base
    ydb/core/protos
    yql/essentials/ast
    ydb/library/yql/dq/actors
    ydb/public/api/protos
)

END()
