LIBRARY()

GENERATE_ENUM_SERIALIZATION(ddisk_data_copier.h)

SRCS(
    ddisk_data_copier.cpp
    direct_block_group_impl.cpp
    direct_block_group_in_mem.cpp
    direct_block_group_mock.cpp
    direct_block_group.cpp
    erase_request.cpp
    fast_path_service.cpp
    flush_request.cpp
    load_actor_adapter.cpp
    part_database.cpp
    part_initschema.cpp
    part_loadstate.cpp
    part_storepartitionids.cpp
    part_storevolumeconfig.cpp
    partition_direct_actor.cpp
    partition_direct.cpp
    range_translate.cpp
    read_request.cpp
    region.cpp
    restore_request.cpp
    vchunk_config.cpp
    vchunk.cpp
    write_request.cpp
    write_with_direct_replication_request.cpp
    write_with_pb_replication_request.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/bootstrap
    ydb/core/nbs/cloud/blockstore/config/protos
    ydb/core/nbs/cloud/blockstore/libs/storage/api
    ydb/core/nbs/cloud/blockstore/libs/storage/core
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos
    ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport
    ydb/core/nbs/cloud/storage/core/libs/coroutine

    ydb/core/protos
    ydb/library/aclib
    ydb/library/services

    ydb/core/mind/bscontroller
    contrib/libs/opentelemetry-proto
)

END()

RECURSE(
    dirty_map
    model
)

RECURSE_FOR_TESTS(
    partition_ut
    ut
)
