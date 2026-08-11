#include "dbs_controller_actor.h"
#include "dbs_controller_database.h"

#include <ydb/core/nbs/cloud/storage/core/libs/actors/helpers.h>

namespace NYdb::NBS::NBlockStore::NStorage::NDbsController {

////////////////////////////////////////////////////////////////////////////////

void TDbsControllerActor::HandleGetNodesForPartitionRequest(
    const TEvDbsControllerPrivate::TEvGetNodesForPartitionRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_INFO_S(
        ctx,
        NKikimrServices::DBS_CONTROLLER,
        "Handle GetNodesForPartition request"
            << ", tabletId: " << ev->Get()->Record.GetPartitionTabletId());

    ExecuteTx(
        ctx,
        CreateTx<TGetNodesForPartition>(
            NBS::NStorage::CreateRequestInfo(
                ev->Sender,
                ev->Cookie,
                MakeIntrusive<TCallContext>()),
            ev->Get()->Record.GetPartitionTabletId()));
}

void TDbsControllerActor::HandleGetPartitionsForNodeRequest(
    const TEvDbsControllerPrivate::TEvGetPartitionsForNodeRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_INFO_S(
        ctx,
        NKikimrServices::DBS_CONTROLLER,
        "Handle GetPartitionsForNode request" << ", nodeId: "
                                              << ev->Get()->Record.GetNodeId());

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

bool TDbsControllerActor::PrepareGetNodesForPartition(
    const NActors::TActorContext& ctx,
    NKikimr::NTabletFlatExecutor::TTransactionContext& tx,
    TTxDbsController::TGetNodesForPartition& args)
{
    Y_UNUSED(ctx);

    TDbsControllerDatabase db(tx.DB);

    return db.GetNodesForTablet(args.TabletId, args.Nodes);
}

void TDbsControllerActor::ExecuteGetNodesForPartition(
    const NActors::TActorContext& ctx,
    NKikimr::NTabletFlatExecutor::TTransactionContext& tx,
    TTxDbsController::TGetNodesForPartition& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TDbsControllerActor::CompleteGetNodesForPartition(
    const NActors::TActorContext& ctx,
    TTxDbsController::TGetNodesForPartition& args)
{
    auto response = std::make_unique<
        TEvDbsControllerPrivate::TEvGetNodesForPartitionResponse>(
        MakeError(S_OK));

    for (const auto nodeId: args.Nodes) {
        response->Record.AddNodes(nodeId);
    }

    Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

bool TDbsControllerActor::PrepareGetPartitionsForNode(
    const NActors::TActorContext& ctx,
    NKikimr::NTabletFlatExecutor::TTransactionContext& tx,
    TTxDbsController::TGetPartitionsForNode& args)
{
    Y_UNUSED(ctx);

    TDbsControllerDatabase db(tx.DB);

    return db.GetTabletsForNode(args.NodeId, args.Tablets);
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
