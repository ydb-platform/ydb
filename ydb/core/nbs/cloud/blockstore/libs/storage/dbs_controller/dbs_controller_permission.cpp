#include "dbs_controller_actor.h"
#include "dbs_controller_database.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

#include <ydb/core/nbs/cloud/storage/core/libs/actors/helpers.h>

namespace NYdb::NBS::NBlockStore::NStorage::NDbsController {

////////////////////////////////////////////////////////////////////////////////

void TDbsControllerActor::HandleNodeMaintenancePermissionRequest(
    const TEvDbsControllerPrivate::TEvNodeMaintenancePermissionRequest::TPtr&
        ev,
    const NActors::TActorContext& ctx)
{
    TVector<ui32> nodeIds{
        ev->Get()->Record.GetNodeIds().begin(),
        ev->Get()->Record.GetNodeIds().end()};
    ExecuteTx(
        ctx,
        CreateTx<TNodeMaintenancePermission>(
            NBS::NStorage::CreateRequestInfo(
                ev->Sender,
                ev->Cookie,
                MakeIntrusive<TCallContext>()),
            nodeIds));
}

void TDbsControllerActor::HandleDiskMaintenancePermissionRequest(
    const TEvDbsControllerPrivate::TEvDiskMaintenancePermissionRequest::TPtr&
        ev,
    const NActors::TActorContext& ctx)
{
    auto response = std::make_unique<
        TEvDbsControllerPrivate::TEvDiskMaintenancePermissionResponse>(
        MakeError(E_NOT_IMPLEMENTED));
    Reply(ctx, *ev, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

bool TDbsControllerActor::PrepareNodeMaintenancePermission(
    const NActors::TActorContext& ctx,
    NKikimr::NTabletFlatExecutor::TTransactionContext& tx,
    TTxDbsController::TNodeMaintenancePermission& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);

    TDbsControllerDatabase db(tx.DB);

    THashMap<TDbsControllerDatabase::TDirectKey, ui64> dependentDbgs;

    if (!db.GetAffectedDBGsWithNodeCounts(args.NodeIds, dependentDbgs)) {
        return false;
    }

    const ui64 tolerableNodesCount = DirectBlockGroupHostCount - 1;

    TSet<ui64> blockingTablets;
    args.Allowed = true;
    for (const auto& [key, nodesCount]: dependentDbgs) {
        if (nodesCount < tolerableNodesCount) {
            args.Allowed = false;
            blockingTablets.insert(std::get<0>(key));
        }
    }

    args.BlockingTablets.assign(blockingTablets.begin(), blockingTablets.end());

    return true;
}

void TDbsControllerActor::ExecuteNodeMaintenancePermission(
    const NActors::TActorContext& ctx,
    NKikimr::NTabletFlatExecutor::TTransactionContext& tx,
    TTxDbsController::TNodeMaintenancePermission& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TDbsControllerActor::CompleteNodeMaintenancePermission(
    const NActors::TActorContext& ctx,
    TTxDbsController::TNodeMaintenancePermission& args)
{
    if (args.Allowed) {
        LOG_INFO_S(
            ctx,
            NKikimrServices::DBS_CONTROLLER,
            "NodeMaintenancePermission allowed for nodes " << args.NodeIds);
    } else {
        LOG_INFO_S(
            ctx,
            NKikimrServices::DBS_CONTROLLER,
            "NodeMaintenancePermission denied for nodes "
                << args.NodeIds
                << ", blocked by partitions: " << args.BlockingTablets);
    }

    auto response = std::make_unique<
        TEvDbsControllerPrivate::TEvNodeMaintenancePermissionResponse>(
        MakeError(S_OK));

    response->Record.SetDecision(
        args.Allowed ? NProto::EDecision::ALLOW : NProto::EDecision::DENY);

    for (const auto tabletId: args.BlockingTablets) {
        response->Record.AddBlockingPartitionIds(tabletId);
    }

    Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController

template <>
inline void Out<TVector<ui64>>(IOutputStream& o, const TVector<ui64>& vec)
{
    o << "[ ";
    bool isFirst = true;
    for (const auto& x: vec) {
        if (!isFirst) {
            o << ", ";
        }
        isFirst = false;
        o << x << ", ";
    }
    o << "]";
}

template <>
inline void Out<TVector<ui32>>(IOutputStream& o, const TVector<ui32>& vec)
{
    o << "[ ";
    bool isFirst = true;
    for (const auto& x: vec) {
        if (!isFirst) {
            o << ", ";
        }
        isFirst = false;
        o << x;
    }
    o << "]";
}
