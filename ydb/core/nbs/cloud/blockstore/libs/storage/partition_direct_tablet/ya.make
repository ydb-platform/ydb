LIBRARY()

SRCS(
    delete_partition.cpp
    load_actor_adapter.cpp
    part_add_host_to_dbg.cpp
    part_database.cpp
    part_initschema.cpp
    part_loadstate.cpp
    part_monitoring.cpp
    part_storepartitionids.cpp
    part_storevolumeconfig.cpp
    part_updatedirtymapstate.cpp
    part_updatevchunkconfig.cpp
    partition_cleanup_actor.cpp
    partition_direct.cpp
    partition_direct_actor.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/bootstrap
    ydb/core/nbs/cloud/blockstore/config/protos
    ydb/core/nbs/cloud/blockstore/libs/diagnostics
    ydb/core/nbs/cloud/blockstore/libs/storage/api
    ydb/core/nbs/cloud/blockstore/libs/storage/core
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/mon_page
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos
    ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport
    ydb/core/nbs/cloud/storage/core/libs/coroutine

    ydb/core/protos
    ydb/library/services

    ydb/core/mind/bscontroller

    library/cpp/cgiparam
)

END()

RECURSE_FOR_TESTS(
    partition_ut
)
