LIBRARY()

SRCS(
    config.cpp
)

PEERDIR(
    ydb/core/fq/libs/actors/logging
    ydb/core/fq/libs/compute/common
    ydb/core/fq/libs/config/protos
    ydb/core/fq/libs/control_plane_storage
    ydb/public/api/protos
)

YQL_LAST_ABI_VERSION()

END()
