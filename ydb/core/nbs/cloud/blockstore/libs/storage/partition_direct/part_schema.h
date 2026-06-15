#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/tablet_schema.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/partition_direct.pb.h>

#include <ydb/core/protos/blockstore_config.pb.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct TPartitionSchema: public NKikimr::NIceDb::Schema
{
    enum EChannels
    {
        SystemChannel,
        LogChannel,
        IndexChannel,
    };

    struct TabletInfo: public TTableSchema<1>
    {
        struct Id: public Column<1, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct StorageConfig
            : public Column<2, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = NProto::TStorageServiceConfig;
        };

        struct VolumeConfig
            : public Column<3, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = NKikimrBlockStore::TVolumeConfig;
        };

        struct DirectBlockGroupsConnections
            : public Column<4, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = ::NYdb::NBS::PartitionDirect::NProto::
                TDirectBlockGroupsConnections;
        };

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<
            Id,
            StorageConfig,
            VolumeConfig,
            DirectBlockGroupsConnections>;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };

    // Persisted vchunk config overrides, keyed by vchunk index. Only vchunks
    // whose layout was explicitly updated have a row here.
    struct VChunkConfigs: public TTableSchema<2>
    {
        struct VChunkIndex: public Column<1, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct Config: public Column<2, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = ::NYdb::NBS::PartitionDirect::NProto::TVChunkConfig;
        };

        using TKey = TableKey<VChunkIndex>;
        using TColumns = TableColumns<VChunkIndex, Config>;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };

    using TTables = SchemaTables<TabletInfo, VChunkConfigs>;

    using TSettings =
        SchemaSettings<ExecutorLogBatching<true>, ExecutorLogFlushPeriod<0>>;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
