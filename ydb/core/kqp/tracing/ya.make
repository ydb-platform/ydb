LIBRARY()

SRCS(
    query_description.cpp
    user_facing.cpp
    user_facing_renderer.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/kqp/common
    ydb/core/kqp/common/compilation
    ydb/core/kqp/common/events
    ydb/core/kqp/common/simple
    ydb/core/protos
    ydb/library/actors/core
    ydb/library/actors/wilson
    ydb/library/security
    ydb/library/wilson_ids
    ydb/library/yql/dq/actors/protos
    ydb/public/api/protos
)

END()
