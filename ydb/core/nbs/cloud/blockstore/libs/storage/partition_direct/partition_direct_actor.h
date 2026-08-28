#pragma once

#include "direct_block_group.h"
#include "part_counters.h"
#include "partition_direct_events_private.h"

#include <ydb/core/nbs/cloud/blockstore/config/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/tablet.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/disk_description.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/log_title.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/mon_page/mon_model.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/public.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor_pool.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
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
    TDiskDescription DiskDescription;
    TStorageConfigPtr StorageConfig;
    NKikimrBlockStore::TVolumeConfig VolumeConfig;
    NActors::TActorId BSControllerPipeClient;

    NActors::TActorId LoadActorAdapter;
    bool DDiskBlockGroupAllocated = false;
    TFastPathServicePtr FastPathService;
    // Chaos controllers are indexed by DirectBlockGroup index.
    TVector<NTransport::IChaosInjectorControlPtr> ChaosInjectorControls;

    TDirectBlockGroupsConnections DirectBlockGroupsConnections;

    struct TDeleteWaiter
    {
        NActors::TActorId Sender;
        ui64 Cookie = 0;
    };

    TVector<TDeleteWaiter> InflightDeleteRequests;
    NActors::TActorId CleanupActor;

    struct TAddHostInFlight
    {
        size_t DirectBlockGroupId = 0;
        THostIndex NewHostIndex = InvalidHostIndex;
        NActors::TActorId BSPipeClient;
    };

    // At most one add-host runs at a time across the whole partition.
    std::optional<TAddHostInFlight> AddHostInFlight;

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

public:
    TPartitionActor(
        const NActors::TActorId& tablet,
        NKikimr::TTabletStorageInfo* info);

    ~TPartitionActor() override;

    static constexpr ui32 LogComponent = NKikimrServices::NBS_PARTITION;
    using TCounters = TPartitionCounters;

private:
    void StateInit(TAutoPtr<NActors::IEventHandle>& ev);
    STFUNC(StateWork);
    // Remove tablet and wipe disk
    STFUNC(StateDelete);

    // Common handlers in different states
    void HandleCommonEvents(TAutoPtr<NActors::IEventHandle>& ev);

    void PassAway() override;

    // The tablet's own monitoring page, reached via the standard tablet page's
    // "App" link. The base class passes a null event to ask whether that link
    // should appear - it always should.
    bool OnRenderAppHtmlPage(
        NActors::NMon::TEvRemoteHttpInfo::TPtr ev,
        const NActors::TActorContext& ctx) override;

    void OnDetach(const NActors::TActorContext& ctx) override;
    void OnTabletDead(
        NKikimr::TEvTablet::TEvTabletDead::TPtr& ev,
        const NActors::TActorContext& ctx) override;
    void OnActivateExecutor(const NActors::TActorContext& ctx) override;
    void DefaultSignalTabletActive(const NActors::TActorContext& ctx) override;

    void CleanupResources(const NActors::TActorContext& ctx);
    void DetachEndpointAddDie(const NActors::TActorContext& ctx);

    void HandleConnect(
        NKikimr::TEvTabletPipe::TEvClientConnected::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleDisconnect(
        NKikimr::TEvTabletPipe::TEvClientDestroyed::TPtr& ev,
        const NActors::TActorContext& ctx);

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

    void HandleUpdateDirtyMapState(
        const TEvPartitionDirectPrivate::TEvUpdateDirtyMapState::TPtr& ev,
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

    void HandlePoisonByBlockedGeneration(
        const TEvPartitionDirectPrivate::TEvPoison::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleAddHostToDBG(
        const TEvPartitionDirectPrivate::TEvAddHostToDBG::TPtr& ev,
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

    void ReplyToDeleteWaiters(
        const NActors::TActorContext& ctx,
        const NProto::TError& error);

    void FinishDelete(const NActors::TActorContext& ctx);

    // Rejects (logs + notifies the DBG) and returns false if the AddHost
    // request is invalid; true if it may proceed.
    bool ValidateAddHostToDBGRequest(
        const NActors::TActorContext& ctx,
        size_t dbgId,
        THostIndex newHostIndex);
    void RejectAddHost(
        const NActors::TActorContext& ctx,
        size_t dbgId,
        const TString& message);
    void SendAllocateDDiskForAddHost(
        const NActors::TActorContext& ctx,
        size_t dbgId,
        THostIndex newHostIndex);

    [[nodiscard]] TTabletInfo MakeMonTabletInfo() const;

    [[nodiscard]] TString GetSocketPath() const;

    void Start(
        const NActors::TActorContext& ctx,
        TDirectBlockGroupsConnections directBlockGroupsConnections,
        const TVChunkConfigs& vChunkConfigs,
        const TDirtyMapStateProtos& dirtyMapStates);

    TVector<IDirectBlockGroupPtr> CreateDirectBlockGroups(
        TDirectBlockGroupsConnections directBlockGroupsConnections);

    BLOCKSTORE_PARTITION_TRANSACTIONS(
        BLOCKSTORE_IMPLEMENT_TRANSACTION,
        TTxPartition)
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
