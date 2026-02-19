LIBRARY()

SRCS(
    ss_proxy.cpp
    ss_proxy_actor.cpp
    ss_proxy_actor_createvolume.cpp
    ss_proxy_actor_describescheme.cpp
    ss_proxy_actor_modifyscheme.cpp
    ss_proxy_actor_waitschemetx.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/kikimr
    ydb/core/nbs/cloud/blockstore/libs/storage/api
    ydb/core/nbs/cloud/blockstore/libs/storage/core
    ydb/core/nbs/cloud/storage/core/libs/kikimr

    ydb/core/base
    ydb/core/tablet
    ydb/core/tx/schemeshard
    ydb/core/tx/tx_proxy

    ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(
    ut
)
