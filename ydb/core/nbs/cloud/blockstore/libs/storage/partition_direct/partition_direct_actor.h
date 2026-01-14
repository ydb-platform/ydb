#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>

#include <ydb/core/base/tablet.h>
#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/log.h>


namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NKikimr;

class TPartitionActor
    : public NActors::TActor<TPartitionActor>,
      public NKikimr::NTabletFlatExecutor::TTabletExecutedFlat
{
public:
    TPartitionActor(
        const NActors::TActorId& owner,
        TTabletStorageInfo* storage,
        const NActors::TActorId& volumeActorId,
        ui64 volumeTabletId);

    static TString GetStateName(ui32 state);

private:
    void OnActivateExecutor(const TActorContext& ctx) override;
    void OnDetach(const TActorContext& ctx) override;
    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr& ev, const TActorContext& ctx) override;
    void DefaultSignalTabletActive(const TActorContext& ctx) override;
    void Enqueue(STFUNC_SIG) override;

    STFUNC(StateWork);

    void HandleWaitReady(
        const TEvPartition::TEvWaitReadyRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWakeup(
        const TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);

    // PipeCache event handlers
    void HandleDeliveryProblem(
        const TEvPipeCache::TEvDeliveryProblem::TPtr& ev,
        const NActors::TActorContext& ctx);

    virtual void HandleReadBlocksLocalRequest(
        const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    virtual void HandleWriteBlocksLocalRequest(
        const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
        const NActors::TActorContext& ctx);
};

} // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
