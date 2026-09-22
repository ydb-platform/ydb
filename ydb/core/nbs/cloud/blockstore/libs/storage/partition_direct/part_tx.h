#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/partition_direct.pb.h>
<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_tx.h
=======
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/model/touched_vchunks.h>
>>>>>>> e33198cc8e8 ([NBS-7709] Persist VChunk touching (#53346)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/part_tx.h

#include <ydb/core/protos/blobstorage_ddisk.pb.h>
#include <ydb/core/protos/blockstore_config.pb.h>

#include <ydb/library/actors/core/actorid.h>

#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_tx.h
#define BLOCKSTORE_PARTITION_TRANSACTIONS(xxx, ...) \
    xxx(InitSchema, __VA_ARGS__)                    \
    xxx(LoadState, __VA_ARGS__)                     \
    xxx(StoreVolumeConfig, __VA_ARGS__)             \
    xxx(StorePartitionIds, __VA_ARGS__)             \
    xxx(UpdateVChunkConfig, __VA_ARGS__)            \
    xxx(StartAddHost, __VA_ARGS__)                  \
    xxx(AddHostToDBG, __VA_ARGS__)                  \
=======
#define BLOCKSTORE_PARTITION_TRANSACTIONS(xxx, ...)                            \
    xxx(InitSchema, __VA_ARGS__)                                               \
    xxx(LoadState, __VA_ARGS__)                                                \
    xxx(StoreVolumeConfig, __VA_ARGS__)                                        \
    xxx(StorePartitionIds, __VA_ARGS__)                                        \
    xxx(UpdateVChunkState, __VA_ARGS__)                                        \
    xxx(SetVChunkTouched, __VA_ARGS__)                                         \
    xxx(StartAddHost, __VA_ARGS__)                                             \
    xxx(AddHostToDBG, __VA_ARGS__)                                             \
    xxx(StartRemoveHost, __VA_ARGS__)                                          \
    xxx(CommitRemoveHost, __VA_ARGS__)                                         \
>>>>>>> ed0d0ddf015 (NBS-6784: Remove host from dbg (#46061)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/part_tx.h
    xxx(Monitoring, __VA_ARGS__)

// BLOCKSTORE_PARTITION_TRANSACTIONS

////////////////////////////////////////////////////////////////////////////////

struct TTxPartition
{
    using TDirectBlockGroupsConnections =
        ::NYdb::NBS::PartitionDirect::NProto::TDirectBlockGroupsConnections;
    using TAddHostInProgress =
        ::NYdb::NBS::PartitionDirect::NProto::TAddHostInProgress;
    using TRemoveHostInProgress =
        ::NYdb::NBS::PartitionDirect::NProto::TRemoveHostInProgress;

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
<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_tx.h
        TVector<TVChunkConfig> VChunkConfigs;
=======
        TVChunkConfigs VChunkConfigs;
        TDirtyMapStateProtos DirtyMapStates;
        TTouchedVChunks TouchedVChunks;
>>>>>>> e33198cc8e8 ([NBS-7709] Persist VChunk touching (#53346)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/part_tx.h
        TMaybe<TAddHostInProgress> AddHostInProgress;
        TMaybe<TRemoveHostInProgress> RemoveHostInProgress;

        explicit TLoadState()
        {}

        void Clear()
        {
            VolumeConfig.Clear();
            DirectBlockGroupsConnections.Clear();
            VChunkConfigs.clear();
<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_tx.h
=======
            DirtyMapStates.clear();
            TouchedVChunks = {};
>>>>>>> e33198cc8e8 ([NBS-7709] Persist VChunk touching (#53346)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/part_tx.h
            AddHostInProgress.Clear();
            RemoveHostInProgress.Clear();
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
    // TUpdateVChunkState
    //
<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_tx.h
    struct TUpdateVChunkConfig
    {
        const TVChunkConfig VChunkConfig;

        explicit TUpdateVChunkConfig(TVChunkConfig vChunkConfig)
            : VChunkConfig(std::move(vChunkConfig))
=======
    struct TUpdateVChunkState
    {
        struct TUpdateStateRequest
        {
            ui32 VChunkIndex;
            TVChunkConfig VChunkConfig;
            TDirtyMapStateProto DirtyMapState;
            TPersistResultPromise UpdateCompleted;
        };

        using TUpdateStateRequests = TVector<TUpdateStateRequest>;

        TUpdateStateRequests UpdateStateRequests;

        explicit TUpdateVChunkState(TUpdateStateRequests updateStateRequests)
            : UpdateStateRequests(std::move(updateStateRequests))
>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/part_tx.h
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // SetVChunkTouched
    //
    struct TSetVChunkTouched
    {
        TVector<TTouchedVChunks::TChunk> Chunks;

        explicit TSetVChunkTouched(TVector<TTouchedVChunks::TChunk> chunks)
            : Chunks(std::move(chunks))
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
<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_tx.h
        const THostIndex NewHostIndex;

        TStartAddHost(size_t directBlockGroupId, THostIndex newHostIndex)
            : DirectBlockGroupId(directBlockGroupId)
            , NewHostIndex(newHostIndex)
=======
        const ui32 LiveHostCount;
        const ui32 DBGConnectionsConfigGeneration;

        TStartAddHost(
            size_t directBlockGroupId,
            ui32 liveHostCount,
            ui32 dbgConnectionsConfigGeneration)
            : DirectBlockGroupId(directBlockGroupId)
            , LiveHostCount(liveHostCount)
            , DBGConnectionsConfigGeneration(dbgConnectionsConfigGeneration)
>>>>>>> ed0d0ddf015 (NBS-6784: Remove host from dbg (#46061)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/part_tx.h
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
    // TStartRemoveHost
    //
    struct TStartRemoveHost
    {
        const TRemoveHostInProgress Intent;

        explicit TStartRemoveHost(TRemoveHostInProgress intent)
            : Intent(std::move(intent))
        {}

        void Clear()
        {}
    };

    struct TCommitRemoveHost
    {
        const TDirectBlockGroupsConnections DirectBlockGroupsConnections;
        const size_t DirectBlockGroupId;
        const THostIndex RemoveIndex;

        TCommitRemoveHost(
            TDirectBlockGroupsConnections directBlockGroupsConnections,
            size_t directBlockGroupId,
            THostIndex removeIndex)
            : DirectBlockGroupsConnections(
                  std::move(directBlockGroupsConnections))
            , DirectBlockGroupId(directBlockGroupId)
            , RemoveIndex(removeIndex)
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
<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_tx.h
        TVector<TVChunkConfig> VChunkConfigs;
=======
>>>>>>> 7af514ad642 ([NBS-7516] Extend the partition direct overview page with actual storage placement for touched VChunks (#53472)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/part_tx.h

        explicit TMonitoring(NActors::TActorId requester)
            : Requester(requester)
        {}

        void Clear()
        {
            VolumeConfig.Clear();
            DirectBlockGroupsConnections.Clear();
            AddHostInProgress.Clear();
        }
    };
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
