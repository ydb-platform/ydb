LIBRARY()

SRCS(
    request_info.cpp
    volume_label.cpp
    volume_model.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/config
    ydb/core/nbs/cloud/blockstore/libs/storage/model
    ydb/core/nbs/cloud/storage/core/libs/common
    ydb/core/nbs/cloud/storage/core/protos
    ydb/core/control
    ydb/core/protos

    library/cpp/deprecated/atomic
    library/cpp/lwtrace
)

END()
