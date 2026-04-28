LIBRARY()

SRCS(
    request_info.cpp
    tablet.cpp
    tablet_schema.cpp
    volume_label.cpp
    volume_model.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/nbs/cloud/blockstore/libs/storage/model
    ydb/core/nbs/cloud/storage/core/libs/common
    ydb/core/nbs/cloud/storage/core/protos
    ydb/core/control
    ydb/core/engine/minikql
    ydb/core/protos
    ydb/core/tablet_flat

    library/cpp/deprecated/atomic
    library/cpp/lwtrace
)

END()

RECURSE(ut)
