LIBRARY()

SRCS(
    server.cpp
    vhost.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/common
    ydb/core/nbs/cloud/blockstore/libs/diagnostics
    ydb/core/nbs/cloud/blockstore/libs/service

    ydb/core/nbs/cloud/storage/core/libs/common

    ydb/core/nbs/cloud/contrib/vhost

    ydb/library/actors/core

    library/cpp/logger
    library/cpp/unified_agent_client
)

END()

RECURSE_FOR_TESTS(
    ut
    ut_stress
)
