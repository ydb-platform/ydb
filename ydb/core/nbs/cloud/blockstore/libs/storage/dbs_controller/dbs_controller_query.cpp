#include "dbs_controller_actor.h"
#include "dbs_controller_database.h"

#include <ydb/core/nbs/cloud/storage/core/libs/actors/helpers.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::DBS_CONTROLLER

namespace NYdb::NBS::NBlockStore::NStorage::NDbsController {

////////////////////////////////////////////////////////////////////////////////

void TDbsControllerActor::HandleGetPartitionsForNodeRequest(
    const TEvDbsControllerPrivate::TEvGetPartitionsForNodeRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    YDB_LOG_INFO_CTX(ctx, "Handle GetPartitionsForNode request",
        {"nodeId", ev->Get()->Record.GetNodeId()});

    ExecuteTx(
        ctx,
        CreateTx<TGetPartitionsForNode>(
            NBS::NStorage::CreateRequestInfo(
                ev->Sender,
                ev->Cookie,
                MakeIntrusive<TCallContext>()),
            ev->Get()->Record.GetNodeId()));
}

////////////////////////////////////////////////////////////////////////////////

bool TDbsControllerActor::PrepareGetPartitionsForNode(
    const NActors::TActorContext& ctx,
    NKikimr::NTabletFlatExecutor::TTransactionContext& tx,
    TTxDbsController::TGetPartitionsForNode& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);

    TDbsControllerDatabase db(tx.DB);

    return db.GetPartitionsForNode(args.NodeId, args.Tablets);
}

void TDbsControllerActor::ExecuteGetPartitionsForNode(
    const NActors::TActorContext& ctx,
    NKikimr::NTabletFlatExecutor::TTransactionContext& tx,
    TTxDbsController::TGetPartitionsForNode& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TDbsControllerActor::CompleteGetPartitionsForNode(
    const NActors::TActorContext& ctx,
    TTxDbsController::TGetPartitionsForNode& args)
{
    auto response = std::make_unique<
        TEvDbsControllerPrivate::TEvGetPartitionsForNodeResponse>(
        MakeError(S_OK));

    for (const auto tabletId: args.Tablets) {
        response->Record.AddPartitions(tabletId);
    }

    Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
