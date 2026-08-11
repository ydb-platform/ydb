#include "dbs_controller_actor.h"
#include "dbs_controller_database.h"

#include <ydb/core/nbs/cloud/storage/core/libs/actors/helpers.h>

namespace NYdb::NBS::NBlockStore::NStorage::NDbsController {

////////////////////////////////////////////////////////////////////////////////

void TDbsControllerActor::HandleUpdateDDiskMapRequest(
    const TEvDbsControllerPrivate::TEvUpdateDDiskMapRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_INFO_S(
        ctx,
        NKikimrServices::DBS_CONTROLLER,
        "Handle UpdateDDiskMap request" << ", tabletId: "
                                        << ev->Get()->Record.GetTabletId());

    ExecuteTx(
        ctx,
        CreateTx<TUpdateDDiskMap>(
            NBS::NStorage::CreateRequestInfo(
                ev->Sender,
                ev->Cookie,
                MakeIntrusive<TCallContext>()),
            ev->Get()->Record.GetTabletId(),
            ev->Get()->Record.GetPartitionDDisks()));
}

////////////////////////////////////////////////////////////////////////////////

bool TDbsControllerActor::PrepareUpdateDDiskMap(
    const NActors::TActorContext& ctx,
    NKikimr::NTabletFlatExecutor::TTransactionContext& tx,
    TTxDbsController::TUpdateDDiskMap& args)
{
    Y_UNUSED(ctx);

    TDbsControllerDatabase db(tx.DB);

    return db.GetRecordKeysForTablet(
        args.PartitionTabletId,
        args.TabletRecordsKeys);
}

void TDbsControllerActor::ExecuteUpdateDDiskMap(
    const NActors::TActorContext& ctx,
    NKikimr::NTabletFlatExecutor::TTransactionContext& tx,
    TTxDbsController::TUpdateDDiskMap& args)
{
    Y_UNUSED(ctx);

    TDbsControllerDatabase db(tx.DB);

    db.ClearRecords(args.TabletRecordsKeys);
    db.FillTabletRecords(args.PartitionTabletId, args.DDisks);
}

void TDbsControllerActor::CompleteUpdateDDiskMap(
    const NActors::TActorContext& ctx,
    TTxDbsController::TUpdateDDiskMap& args)
{
    LOG_INFO(
        ctx,
        NKikimrServices::DBS_CONTROLLER,
        "UpdateDDiskMap persisted data for tablet %" PRIu64,
        args.PartitionTabletId);

    auto response =
        std::make_unique<TEvDbsControllerPrivate::TEvUpdateDDiskMapResponse>(
            MakeError(S_OK));

    Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
