#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/request_info.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/blockstore/core/blockstore.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/storage/api/volume.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/event_load.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/map.h>

namespace NYdb::NBS::NStorage {

using namespace NActors;
using namespace NKikimr;

class TVolumeActor
    : public TActorBootstrapped<TVolumeActor>
    , NKikimr::NTabletFlatExecutor::TTabletExecutedFlat
{
    enum EState
    {
        STATE_BOOT,
        STATE_INIT,
        STATE_WORK,
        STATE_ZOMBIE,
        STATE_MAX,
    };

    // A pending event sent to the partition and kept until its reply arrives,
    // so it can be sent again when the shared partition pipe fails.
    struct TPendingEvent
    {
        ui32 EventType = 0;
        TIntrusivePtr<TEventSerializedData> Data;
    };

    struct TUpdateVolumeConfigRequest
    {
        TRequestInfoPtr RequestInfo;
        ui64 TxId = 0;
        ui64 PendingEventId = 0;
    };

    THashMap<ui64, TUpdateVolumeConfigRequest>
        UpdateVolumeConfigRequests;   // txId -> request
    // The volume has one partition. Every pending event shares this pipe.
    ui64 PartitionTabletId = 0;
    // Open while PendingEvents is not empty.
    TActorId PartitionPipeClient;
    TMap<ui64, TPendingEvent> PendingEvents;   // pendingEventId -> event
    ui64 NextPendingEventId = 1;

public:
    TVolumeActor(const TActorId& tablet, NKikimr::TTabletStorageInfo* info);
    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void OnDetach(const TActorContext& ctx) override;
    void OnTabletDead(
        TEvTablet::TEvTabletDead::TPtr& ev,
        const TActorContext& ctx) override;
    void OnActivateExecutor(const TActorContext& ctx) override;
    void DefaultSignalTabletActive(const TActorContext& ctx) override;

    void HandleServerConnected(
        const NKikimr::TEvTabletPipe::TEvServerConnected::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleServerDisconnected(
        const NKikimr::TEvTabletPipe::TEvServerDisconnected::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleServerDestroyed(
        const NKikimr::TEvTabletPipe::TEvServerDestroyed::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleClientConnected(
        const NKikimr::TEvTabletPipe::TEvClientConnected::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleClientDestroyed(
        const NKikimr::TEvTabletPipe::TEvClientDestroyed::TPtr& ev,
        const NActors::TActorContext& ctx);

    // Serializes the event, registers it as a pending event, and sends it to
    // the partition. Returns the pending event id used to release it when
    // the reply arrives.
    ui64 SendPendingEventToPartition(
        const NActors::TActorContext& ctx,
        ui64 partitionTabletId,
        std::unique_ptr<IEventBase> event);

    // Opens PartitionPipeClient. The pipe client backs off while the
    // partition is down.
    void OpenPartitionPipe(const NActors::TActorContext& ctx);

    // Closes the failed pipe and sends every pending event on a new one,
    // in send order. A pipe that was already closed or replaced is ignored.
    void ResendPendingEventsToPartition(
        const NActors::TActorContext& ctx,
        const TActorId& pipeClient);

    // Drops the pending event. Closes the pipe when none remain.
    void ReleasePendingEvent(
        const NActors::TActorContext& ctx,
        ui64 pendingEventId);

    void HandleUpdateVolumeConfig(
        const NKikimr::TEvBlockStore::TEvUpdateVolumeConfig::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdateVolumeConfigResponse(
        const NKikimr::TEvBlockStore::TEvUpdateVolumeConfigResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    // TODO: NBS-7763 answer with the volume state; nbsd gets an empty OK.
    void HandleStatVolume(
        const NNbs1CompatApi::NBlockStore::TEvService::TEvStatVolumeRequest::
            TPtr& ev,
        const NActors::TActorContext& ctx);

    // TODO: NBS-7763 wait for the partition; nbsd gets an empty OK.
    void HandleWaitReady(
        const NNbs1CompatApi::NBlockStore::TEvVolume::TEvWaitReadyRequest::TPtr&
            ev,
        const NActors::TActorContext& ctx);

    void ReportTabletState(const TActorContext& ctx);
};

}   // namespace NYdb::NBS::NStorage
