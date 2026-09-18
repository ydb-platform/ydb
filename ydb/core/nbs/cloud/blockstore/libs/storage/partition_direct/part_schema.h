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

        // Set while an AddHost is in flight, cleared once it finishes (or is
        // abandoned). Replayed on restart to recover a half-committed add.
        struct AddHostInProgress
            : public Column<5, NKikimr::NScheme::NTypeIds::String>
        {
            using Type =
                ::NYdb::NBS::PartitionDirect::NProto::TAddHostInProgress;
        };

        // Set while a RemoveHost is in flight, cleared when it commits.
        struct RemoveHostInProgress
            : public Column<6, NKikimr::NScheme::NTypeIds::String>
        {
            using Type =
                ::NYdb::NBS::PartitionDirect::NProto::TRemoveHostInProgress;
        };

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<
            Id,
            StorageConfig,
            VolumeConfig,
            DirectBlockGroupsConnections,
<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_schema.h
            AddHostInProgress>;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
=======
            AddHostInProgress,
            RemoveHostInProgress>;
>>>>>>> ed0d0ddf015 (NBS-6784: Remove host from dbg (#46061)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/part_schema.h
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

<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_schema.h
    using TTables = SchemaTables<TabletInfo, VChunkConfigs>;
=======
    // Persisted dirty map state, keyed by vchunk index. Only vchunks whose
    // dirty map was explicitly updated have a row here.
    struct DirtyMapStates: public TTableSchema<3>
    {
        struct VChunkIndex: public Column<1, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct State: public Column<2, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = ::NYdb::NBS::PartitionDirect::NProto::TDirtyMapState;
        };

        using TKey = TableKey<VChunkIndex>;
        using TColumns = TableColumns<VChunkIndex, State>;
    };

    // Persisted masks of touched vchunks. A mask covers 1024 consecutive
    // vchunks and can only gain set bits.
    struct TouchedVChunks: public TTableSchema<4>
    {
        struct VChunkStartIndex
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct Mask: public Column<2, NKikimr::NScheme::NTypeIds::String>
        {
        };

        using TKey = TableKey<VChunkStartIndex>;
        using TColumns = TableColumns<VChunkStartIndex, Mask>;
    };

    using TTables =
        SchemaTables<TabletInfo, VChunkConfigs, DirtyMapStates, TouchedVChunks>;
>>>>>>> e33198cc8e8 ([NBS-7709] Persist VChunk touching (#53346)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/part_schema.h

    using TSettings =
        SchemaSettings<ExecutorLogBatching<true>, ExecutorLogFlushPeriod<0>>;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
