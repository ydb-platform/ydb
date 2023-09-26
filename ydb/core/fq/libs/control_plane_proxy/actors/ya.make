LIBRARY()

SRCS(
    control_plane_storage_requester_actor.cpp
    query_utils.cpp
    ydb_schema_query_actor.cpp
)

PEERDIR(
    contrib/libs/fmt
    library/cpp/iterator
    ydb/core/fq/libs/common
    ydb/core/fq/libs/control_plane_proxy/events
    ydb/core/fq/libs/control_plane_storage/events
    ydb/core/fq/libs/result_formatter
    ydb/core/kqp/provider
    ydb/library/db_pool/protos
)

GENERATE_ENUM_SERIALIZATION(ydb_schema_query_actor.h)

YQL_LAST_ABI_VERSION()

END()
