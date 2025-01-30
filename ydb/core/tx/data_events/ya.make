LIBRARY()

PEERDIR(
    ydb/core/tablet
    ydb/public/api/grpc/draft
    ydb/public/api/protos
    ydb/core/base
    ydb/core/tx/data_events/common
    ydb/core/tx/sharding
    ydb/core/tx/long_tx_service/public
    ydb/core/tx/scheme_cache
    ydb/core/formats/arrow
    ydb/core/protos
    ydb/library/accessor
    ydb/library/conclusion
    contrib/libs/apache/arrow
    ydb/library/actors/core
    ydb/library/actors/wilson

    # Temporary fix dep ydb/core/tx/columnshard
    ydb/core/tablet_flat/protos
    ydb/core/tablet_flat
    ydb/core/blobstorage/vdisk/protos
    #
)

SRCS(
    shard_writer.cpp
    shards_splitter.cpp
    write_data.cpp
    columnshard_splitter.cpp
)

END()
