#pragma once

#include "direct_block_group.h"
#include "part_counters.h"
#include "partition_direct_events_private.h"

#include <ydb/core/nbs/cloud/blockstore/config/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/tablet.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/log_title.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/mon_page/mon_model.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor_pool.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/blockstore/core/blockstore.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/mind/bscontroller/types.h>
#include <ydb/core/protos/blockstore_config.pb.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>

#include <ydb/library/actors/core/mon.h>
#include <ydb/library/services/services.pb.h>

#include <optional>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TPartitionActor
    : public NActors::TActor<TPartitionActor>
    , public TTabletBase<TPartitionActor>
{
    using TDirectBlockGroupsConnections =
        ::NYdb::NBS::PartitionDirect::NProto::TDirectBlockGroupsConnections;

    enum EState
    {
        STATE_BOOT,
        STATE_INIT,
        STATE_WORK,
        STATE_ZOMBIE,
        STATE_MAX,
    };

private:
    TLogTitle LogTitle;
    TStorageConfigPtr StorageConfig;
    NKikimrBlockStore::TVolumeConfig VolumeConfig;
    NActors::TActorId BSControllerPipeClient;

    NActors::TActorId LoadActorAdapter;
    bool DDiskBlockGroupAllocated = false;
    std::shared_ptr<TFastPathService> FastPathService;

    TDirectBlockGroupsConnections DirectBlockGroupsConnections;

    struct TAddHostInFlight
    {
        size_t DirectBlockGroupId = 0;
<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/partition_direct_actor.h
        THostIndex NewHostIndex = InvalidHostIndex;
=======
        ui32 LiveHostCount = 0;
        ui32 DBGConnectionsConfigGeneration = 0;
>>>>>>> ed0d0ddf015 (NBS-6784: Remove host from dbg (#46061)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/partition_direct_actor.h
        NActors::TActorId BSPipeClient;
    };

    // At most one add-host runs at a time across the whole partition.
    std::optional<TAddHostInFlight> AddHostInFlight;

<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/partition_direct_actor.h
=======
    struct TRemoveHostInFlight
    {
        size_t DirectBlockGroupId = 0;
        NKikimrBlobStorage::NDDisk::TDDiskId DDiskId;
        NKikimrBlobStorage::NDDisk::TDDiskId PBufferId;
        ui32 DBGConnectionsConfigGeneration = 0;
        NActors::TActorId BSPipeClient;
    };

    // At most one remove-host runs at a time; mutually exclusive with
    // AddHostInFlight.
    std::optional<TRemoveHostInFlight> RemoveHostInFlight;

    // Batch persisting of vchunk configs.
    bool ExecutingUpdateVChunkConfig = false;
    TVector<TPersistResultPromise> ExecutingUpdateVChunkConfigPromises;
    TTxPartition::TUpdateVChunkConfig::TUpdateConfigRequests
        PendingUpdateVChunkConfigRequests;

    // Batch persisting of ahead and behind fields.
    bool ExecutingUpdateDirtyMapState = false;
    TVector<TPersistResultPromise> ExecutingUpdateDirtyMapStatePromises;
    TTxPartition::TUpdateDirtyMapState::TUpdateStateRequests
        PendingUpdateDirtyMapStateRequests;

>>>>>>> ed0d0ddf015 (NBS-6784: Remove host from dbg (#46061)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/partition_direct_actor.h
public:
    TPartitionActor(
        const NActors::TActorId& tablet,
        NKikimr::TTabletStorageInfo* info);

    ~TPartitionActor() override;
    void PassAway() override;

    static constexpr ui32 LogComponent = NKikimrServices::NBS_PARTITION;
    using TCounters = TPartitionCounters;

private:
    void StateInit(TAutoPtr<NActors::IEventHandle>& ev);
    STFUNC(StateWork);

    void HandleHttpInfo(
        NActors::NMon::TEvRemoteHttpInfo::TPtr& ev,
        const NActors::TActorContext& ctx);

    void OnDetach(const NActors::TActorContext& ctx) override;
    void OnTabletDead(
        NKikimr::TEvTablet::TEvTabletDead::TPtr& ev,
        const NActors::TActorContext& ctx) override;
    void OnActivateExecutor(const NActors::TActorContext& ctx) override;
    void DefaultSignalTabletActive(const NActors::TActorContext& ctx) override;

    void HandleServerConnected(
        const NKikimr::TEvTabletPipe::TEvServerConnected::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleServerDisconnected(
        const NKikimr::TEvTabletPipe::TEvServerDisconnected::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleServerDestroyed(
        const NKikimr::TEvTabletPipe::TEvServerDestroyed::TPtr& ev,
        const NActors::TActorContext& ctx);

    void ReportTabletState(const NActors::TActorContext& ctx);

    void CreateBSControllerPipeClient(const NActors::TActorContext& ctx);

    void AllocateDDiskBlockGroup(const NActors::TActorContext& ctx);

    void SendAllocateDDiskForAddHost(
        const NActors::TActorContext& ctx,
        size_t dbgId,
        THostIndex newHostIndex);

    void HandleControllerAllocateDDiskBlockGroupResult(
        const NKikimr::TEvBlobStorage::
            TEvControllerAllocateDDiskBlockGroupResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    // Sets up the group from the first (bulk) allocation response.
    void HandleInitialAllocationResult(
        const NKikimr::TEvBlobStorage::
            TEvControllerAllocateDDiskBlockGroupResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    // Applies a single add-host allocation response: validate, append the new
    // connection, and persist it via TAddHostToDBG.
    void HandleAddHostAllocationResult(
        const NKikimr::TEvBlobStorage::
            TEvControllerAllocateDDiskBlockGroupResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    // Sends the in-flight remove's deletion to BSController.
    void SendRemoveHostRequest(const NActors::TActorContext& ctx);

    // Applies the deletion response; NOT_FOUND means already applied.
    void HandleRemoveHostAllocationResult(
        const NKikimr::TEvBlobStorage::
            TEvControllerAllocateDDiskBlockGroupResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleGetLoadActorAdapterActorId(
        const NYdb::NBS::NBlockStore::TEvService::
            TEvGetLoadActorAdapterActorIdRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdateVolumeConfig(
        const NKikimr::TEvBlockStore::TEvUpdateVolumeConfig::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdateVChunkConfig(
        const TEvPartitionDirectPrivate::TEvUpdateVChunkConfig::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleFastPathServiceReady(
        const TEvPartitionDirectPrivate::TEvFastPathServiceReady::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleFastPathServiceShutdown(
        const TEvPartitionDirectPrivate::TEvFastPathServiceShutdown::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleFastPathServiceStopped(
        const TEvPartitionDirectPrivate::TEvFastPathServiceStopped::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleAddHostToDBG(
        const TEvPartitionDirectPrivate::TEvAddHostToDBG::TPtr& ev,
        const NActors::TActorContext& ctx);

<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/partition_direct_actor.h
=======
    void HandlePersistHostHealth(
        const TEvPartitionDirectPrivate::TEvPersistHostHealth::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleDeletePartition(
        const NYdb::NBS::NBlockStore::TEvService::TEvDeletePartitionRequest::
            TPtr& ev,
        const NActors::TActorContext& ctx);

    void StartPartitionTeardown(const NActors::TActorContext& ctx);

    void HandleFastPathServiceStoppedDuringDelete(
        const TEvPartitionDirectPrivate::TEvFastPathServiceStopped::TPtr& ev,
        const NActors::TActorContext& ctx);

    void StartCleanupActor(
        const NActors::TActorContext& ctx,
        TDirectBlockGroupsConnections connections);

    void HandlePartitionCleanupCompleted(
        const TEvPartitionDirectPrivate::TEvPartitionCleanupCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleAllocateResultDuringDelete(
        const NKikimr::TEvBlobStorage::
            TEvControllerAllocateDDiskBlockGroupResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdateVolumeConfigDuringDelete(
        const NKikimr::TEvBlockStore::TEvUpdateVolumeConfig::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdateVChunkConfigDuringDelete(
        const TEvPartitionDirectPrivate::TEvUpdateVChunkConfig::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdateDirtyMapStateDuringDelete(
        const TEvPartitionDirectPrivate::TEvUpdateDirtyMapState::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleFastPathServiceShutdownDuringDelete(
        const TEvPartitionDirectPrivate::TEvFastPathServiceShutdown::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleAddHostToDBGDuringDelete(
        const TEvPartitionDirectPrivate::TEvAddHostToDBG::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePersistHostHealthDuringDelete(
        const TEvPartitionDirectPrivate::TEvPersistHostHealth::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleRemoveHostFromDBGDuringDelete(
        const TEvPartitionDirectPrivate::TEvRemoveHostFromDBG::TPtr& ev,
        const NActors::TActorContext& ctx);

    void ReplyToDeleteWaiters(
        const NActors::TActorContext& ctx,
        const NProto::TError& error);

    void FinishDelete(const NActors::TActorContext& ctx);

>>>>>>> 8a60344d593 (NBS-7513:  Add Oracle->PartitionDirect PersistHostHealth path (#52610)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/partition_direct_actor.h
    // Rejects (logs + notifies the DBG) and returns false if the AddHost
    // request is invalid; true if it may proceed.
    bool ValidateAddHostToDBGRequest(
        const NActors::TActorContext& ctx,
        size_t dbgId);

    void RejectAddHost(
        const NActors::TActorContext& ctx,
        size_t dbgId,
        const TString& message);
<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/partition_direct_actor.h
=======
    void SendAllocateDDiskForAddHost(
        const NActors::TActorContext& ctx,
        size_t dbgId);

    void HandleRemoveHostFromDBG(
        const TEvPartitionDirectPrivate::TEvRemoveHostFromDBG::TPtr& ev,
        const NActors::TActorContext& ctx);

    bool ValidateRemoveHostFromDBGRequest(
        const NActors::TActorContext& ctx,
        size_t dbgId,
        size_t hostIndex,
        ui32 dbgConnectionsConfigGeneration);

    void RejectRemoveHost(
        const NActors::TActorContext& ctx,
        size_t dbgId,
        size_t hostIndex,
        const TString& message);
>>>>>>> ed0d0ddf015 (NBS-6784: Remove host from dbg (#46061)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/partition_direct_actor.h

    TTabletInfo MakeMonTabletInfo();

    void Start(
        const NActors::TActorContext& ctx,
        TDirectBlockGroupsConnections directBlockGroupsConnections,
        TVector<TVChunkConfig> vChunkConfigs);

    TVector<IDirectBlockGroupPtr> CreateDirectBlockGroups(
        TDirectBlockGroupsConnections directBlockGroupsConnections);

    BLOCKSTORE_PARTITION_TRANSACTIONS(
        BLOCKSTORE_IMPLEMENT_TRANSACTION,
        TTxPartition)
};

////////////////////////////////////////////////////////////////////////////////

<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/partition_direct_actor.h
=======
struct TAllocationResponse
{
    NProto::TError Error;
    const NKikimrBlobStorage::TEvControllerAllocateDDiskBlockGroupResult::
        TDirectBlockGroup* Group = nullptr;
};

[[nodiscard]] TAllocationResponse ValidateAllocationResponse(
    const NKikimr::TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult&
        msg,
    size_t dbgId,
    size_t expectedHostCount);

// Hosts that are not marked RemovedFromBSC.
[[nodiscard]] size_t LiveHostCount(
    const ::NYdb::NBS::PartitionDirect::NProto::TDirectBlockGroupConnections&
        connections);

>>>>>>> ed0d0ddf015 (NBS-6784: Remove host from dbg (#46061)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/partition_direct_actor.h
}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
