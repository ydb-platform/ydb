LIBRARY()

SRCS(
    volume.cpp
    volume_actor.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/kikimr
    ydb/core/nbs/cloud/blockstore/libs/service
    ydb/core/nbs/cloud/blockstore/libs/storage/api
    ydb/core/nbs/cloud/blockstore/libs/storage/core
    ydb/core/nbs/cloud/storage/core/libs/common
    ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/storage/api

    ydb/library/actors/core
    library/cpp/lwtrace
    library/cpp/monlib/service/pages
    library/cpp/protobuf/util

    ydb/core/base
    ydb/core/blockstore/core
    ydb/core/mind
    ydb/core/node_whiteboard
    ydb/core/scheme
    ydb/core/tablet
    ydb/core/tablet_flat
)

END()

RECURSE_FOR_TESTS(
    volume_ut
)
