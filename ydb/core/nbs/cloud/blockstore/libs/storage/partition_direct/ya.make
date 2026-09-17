LIBRARY()

GENERATE_ENUM_SERIALIZATION(ddisk_data_copier.h)
GENERATE_ENUM_SERIALIZATION(dbg_connections.h)
GENERATE_ENUM_SERIALIZATION(partition_direct_service.h)

SRCS(
    dbg_connections.cpp
    ddisk_data_copier.cpp
    direct_block_group_impl.cpp
    direct_block_group.cpp
    erase_request.cpp
    fast_path_service.cpp
    flush_request.cpp
    read_request_executor.cpp
    read_request_multiple_location.cpp
    read_request_single_location.cpp
    region.cpp
    restore_request.cpp
    vchunk.cpp
    write_request_bundle.cpp
    write_request.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/bootstrap
    ydb/core/nbs/cloud/blockstore/config/protos
    ydb/core/nbs/cloud/blockstore/libs/diagnostics
    ydb/core/nbs/cloud/blockstore/libs/storage/api
    ydb/core/nbs/cloud/blockstore/libs/storage/core
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/mon_page
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos
    ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport
    ydb/core/nbs/cloud/blockstore/libs/throttling
    ydb/core/nbs/cloud/storage/core/libs/coroutine

    ydb/core/protos
    ydb/library/aclib
    ydb/library/services

    ydb/core/mind/bscontroller
    contrib/proto/opentelemetry

    library/cpp/cgiparam
)

END()

RECURSE(
    dirty_map
    model
    mon_page
)

RECURSE_FOR_TESTS(
    benchmark
    ut
)
