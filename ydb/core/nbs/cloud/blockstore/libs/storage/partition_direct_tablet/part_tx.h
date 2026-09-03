#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/partition_direct_service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/dirty_map.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/partition_direct.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/public.h>

#include <ydb/core/protos/blobstorage_ddisk.pb.h>
#include <ydb/core/protos/blockstore_config.pb.h>

#include <ydb/library/actors/core/actorid.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_PARTITION_TRANSACTIONS(xxx, ...)                            \
    xxx(InitSchema, __VA_ARGS__)                                               \
    xxx(LoadState, __VA_ARGS__)                                                \
    xxx(StoreVolumeConfig, __VA_ARGS__)                                        \
    xxx(StorePartitionIds, __VA_ARGS__)                                        \
    xxx(UpdateVChunkConfig, __VA_ARGS__)                                       \
    xxx(UpdateDirtyMapState, __VA_ARGS__)                                      \
    xxx(StartAddHost, __VA_ARGS__)                                             \
    xxx(AddHostToDBG, __VA_ARGS__)                                             \
    xxx(Monitoring, __VA_ARGS__)

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
        TVChunkConfigs VChunkConfigs;
        TDirtyMapStateProtos DirtyMapStates;
        TMaybe<TAddHostInProgress> AddHostInProgress;

        void Clear()
        {
            VolumeConfig.Clear();
            DirectBlockGroupsConnections.Clear();
            VChunkConfigs.clear();
            DirtyMapStates.clear();
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
        struct TUpdateConfigRequest
        {
            TVChunkConfig VChunkConfig;
            TPersistResultPromise UpdateCompleted;
        };

        using TUpdateConfigRequests = TVector<TUpdateConfigRequest>;

        TUpdateConfigRequests UpdateConfigRequests;

        explicit TUpdateVChunkConfig(TUpdateConfigRequests updateConfigRequests)
            : UpdateConfigRequests(std::move(updateConfigRequests))
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // TUpdateDirtyMapState
    //
    struct TUpdateDirtyMapState
    {
        struct TUpdateStateRequest
        {
            ui32 VChunkIndex;
            TDirtyMapStateProto State;
            TPersistResultPromise UpdateCompleted;
        };

        using TUpdateStateRequests = TVector<TUpdateStateRequest>;

        TUpdateStateRequests UpdateStateRequests;

        explicit TUpdateDirtyMapState(TUpdateStateRequests updateStateRequests)
            : UpdateStateRequests(std::move(updateStateRequests))
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
        const ui64 Generation;

        TStartAddHost(
            size_t directBlockGroupId,
            THostIndex newHostIndex,
            ui64 generation)
            : DirectBlockGroupId(directBlockGroupId)
            , NewHostIndex(newHostIndex)
            , Generation(generation)
        {}

        void Clear()
        {}
    };

    struct TAddHostToDBG
    {
        const TDirectBlockGroupsConnections DirectBlockGroupsConnections;
        const size_t DirectBlockGroupId;
        const THostIndex NewHostIndex;

        TAddHostToDBG(
            TDirectBlockGroupsConnections directBlockGroupsConnections,
            size_t directBlockGroupId,
            THostIndex newHostIndex)
            : DirectBlockGroupsConnections(
                  std::move(directBlockGroupsConnections))
            , DirectBlockGroupId(directBlockGroupId)
            , NewHostIndex(newHostIndex)
        {}

        void Clear()
        {}
    };

    //
    // Monitoring: read the local DB contents for the mon page.
    //
    struct TMonitoring
    {
        const NActors::TActorId Requester;

        // Filled by Prepare.
        TMaybe<NKikimrBlockStore::TVolumeConfig> VolumeConfig;
        TMaybe<TDirectBlockGroupsConnections> DirectBlockGroupsConnections;
        TMaybe<TAddHostInProgress> AddHostInProgress;
        TVChunkConfigs VChunkConfigs;

        explicit TMonitoring(NActors::TActorId requester)
            : Requester(requester)
        {}

        void Clear()
        {
            VolumeConfig.Clear();
            DirectBlockGroupsConnections.Clear();
            AddHostInProgress.Clear();
            VChunkConfigs.clear();
        }
    };
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
