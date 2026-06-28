#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/partition_direct.pb.h>

#include <ydb/core/protos/blobstorage_ddisk.pb.h>
#include <ydb/core/protos/blockstore_config.pb.h>

#include <ydb/library/actors/core/actorid.h>

#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_PARTITION_TRANSACTIONS(xxx, ...) \
    xxx(InitSchema, __VA_ARGS__)                    \
    xxx(LoadState, __VA_ARGS__)                     \
    xxx(StoreVolumeConfig, __VA_ARGS__)             \
    xxx(StorePartitionIds, __VA_ARGS__)             \
    xxx(UpdateVChunkConfig, __VA_ARGS__)            \
    xxx(StartAddHost, __VA_ARGS__)                  \
    xxx(AddHostToDBG, __VA_ARGS__)

// BLOCKSTORE_PARTITION_TRANSACTIONS

////////////////////////////////////////////////////////////////////////////////

struct TTxPartition
{
    using TDirectBlockGroupsConnections =
        ::NYdb::NBS::PartitionDirect::NProto::TDirectBlockGroupsConnections;
    using TAddHostInProgress =
        ::NYdb::NBS::PartitionDirect::NProto::TAddHostInProgress;

    //
    // InitSchema
    //
    struct TInitSchema
    {
        explicit TInitSchema()
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // LoadState
    //
    struct TLoadState
    {
        TMaybe<NKikimrBlockStore::TVolumeConfig> VolumeConfig;
        TMaybe<TDirectBlockGroupsConnections> DirectBlockGroupsConnections;
        TVector<TVChunkConfig> VChunkConfigs;
        TMaybe<TAddHostInProgress> AddHostInProgress;

        explicit TLoadState()
        {}

        void Clear()
        {
            VolumeConfig.Clear();
            DirectBlockGroupsConnections.Clear();
            VChunkConfigs.clear();
            AddHostInProgress.Clear();
        }
    };

    //
    // TStoreVolumeConfig
    //
    struct TStoreVolumeConfig
    {
        const NKikimrBlockStore::TVolumeConfig VolumeConfig;

        explicit TStoreVolumeConfig(
            NKikimrBlockStore::TVolumeConfig volumeConfig)
            : VolumeConfig(std::move(volumeConfig))
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // TStorePartitionIds
    //
    struct TStorePartitionIds
    {
        const ::NYdb::NBS::PartitionDirect::NProto::
            TDirectBlockGroupsConnections DirectBlockGroupsConnections;

        explicit TStorePartitionIds(
            TDirectBlockGroupsConnections directBlockGroupsConnections)
            : DirectBlockGroupsConnections(
                  std::move(directBlockGroupsConnections))
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // TUpdateVChunkConfig
    //
    struct TUpdateVChunkConfig
    {
        const TVChunkConfig VChunkConfig;

        explicit TUpdateVChunkConfig(TVChunkConfig vChunkConfig)
            : VChunkConfig(std::move(vChunkConfig))
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // TStartAddHost
    //
    struct TStartAddHost
    {
        const size_t DirectBlockGroupId;
        const THostIndex NewHostIndex;

        TStartAddHost(size_t directBlockGroupId, THostIndex newHostIndex)
            : DirectBlockGroupId(directBlockGroupId)
            , NewHostIndex(newHostIndex)
        {}

        void Clear()
        {}
    };

    struct TAddHostToDBG
    {
        const TDirectBlockGroupsConnections DirectBlockGroupsConnections;
        const size_t DirectBlockGroupId;
        const THostIndex NewHostIndex;
        const NKikimrBlobStorage::NDDisk::TDDiskId NewDDiskId;
        const NKikimrBlobStorage::NDDisk::TDDiskId NewPBufferId;

        TAddHostToDBG(
            TDirectBlockGroupsConnections directBlockGroupsConnections,
            size_t directBlockGroupId,
            THostIndex newHostIndex,
            NKikimrBlobStorage::NDDisk::TDDiskId newDDiskId,
            NKikimrBlobStorage::NDDisk::TDDiskId newPBufferId)
            : DirectBlockGroupsConnections(
                  std::move(directBlockGroupsConnections))
            , DirectBlockGroupId(directBlockGroupId)
            , NewHostIndex(newHostIndex)
            , NewDDiskId(std::move(newDDiskId))
            , NewPBufferId(std::move(newPBufferId))
        {}

        void Clear()
        {}
    };
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
