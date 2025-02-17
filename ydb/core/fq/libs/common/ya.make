LIBRARY()

GENERATE_ENUM_SERIALIZATION(util.h)

SRCS(
    cache.h
    compression.cpp
    compression.h
    debug_info.cpp
    entity_id.cpp
    entity_id.h
    util.cpp
    rows_proto_splitter.cpp
    rows_proto_splitter.h
)

PEERDIR(
    library/cpp/blockcodecs
    ydb/core/fq/libs/control_plane_storage/events
    ydb/core/fq/libs/events
    yql/essentials/providers/common/structured_token
    yql/essentials/public/issue
    ydb/public/api/protos
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
