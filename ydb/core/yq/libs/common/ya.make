LIBRARY()

SRCS(
    cache.h
    compression.cpp
    compression.h
    debug_info.cpp
    entity_id.cpp
    entity_id.h
    rows_proto_splitter.cpp
    rows_proto_splitter.h
)

PEERDIR(
    library/cpp/blockcodecs
    ydb/core/yq/libs/control_plane_storage/events
    ydb/core/yq/libs/events
    ydb/library/yql/providers/common/structured_token
    ydb/library/yql/public/issue
    ydb/public/api/protos
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
