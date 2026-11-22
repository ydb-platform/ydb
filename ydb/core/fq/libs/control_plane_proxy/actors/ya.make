LIBRARY()

SRCS(
    binding_rename_actor.cpp
    control_plane_storage_requester_actor.cpp
    query_utils.cpp
    ydb_schema_query_actor.cpp
)

PEERDIR(
    contrib/libs/fmt
    library/cpp/iterator
    library/cpp/json
    library/cpp/string_utils/base64
    ydb/core/fq/libs/common
    ydb/core/fq/libs/config
    ydb/core/fq/libs/config/protos
    ydb/core/fq/libs/control_plane_config
    ydb/core/fq/libs/control_plane_proxy/events
    ydb/core/fq/libs/control_plane_proxy/utils
    ydb/core/fq/libs/control_plane_storage/events
    ydb/core/fq/libs/private_client
    ydb/core/fq/libs/ydb
    ydb/core/fq/libs/rate_limiter/events
    ydb/core/fq/libs/result_formatter
    ydb/core/kqp/provider
    ydb/library/db_pool/protos
    ydb/public/sdk/cpp/adapters/issue
    yql/essentials/core
    yql/essentials/parser/pg_catalog
    yql/essentials/parser/pg_wrapper/interface/type_desc
)

GENERATE_ENUM_SERIALIZATION(ydb_schema_query_actor.h)

YQL_LAST_ABI_VERSION()

END()
