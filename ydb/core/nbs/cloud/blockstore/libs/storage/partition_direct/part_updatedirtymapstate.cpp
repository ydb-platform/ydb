#include "partition_direct_actor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_database.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareUpdateDirtyMapState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TUpdateDirtyMapState& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TPartitionActor::ExecuteUpdateDirtyMapState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TUpdateDirtyMapState& args)
{
    Y_UNUSED(ctx);

    TPartitionDatabase db(tx.DB);
    for (const auto& request: args.UpdateStateRequests) {
        db.StoreDirtyMapState(request.VChunkIndex, request.State);
    }
}

void TPartitionActor::CompleteUpdateDirtyMapState(
    const TActorContext& ctx,
    TTxPartition::TUpdateDirtyMapState& args)
{
    for (auto& request: args.UpdateStateRequests) {
        request.UpdateCompleted.SetValue();
    }
    ExecutingUpdateDirtyMapState = false;

    if (!PendingUpdateDirtyMapStateRequests.empty()) {
        LOG_INFO(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s Execute pending UpdateDirtyMapStateRequests %lu",
            LogTitle.GetWithTime().c_str(),
            PendingUpdateDirtyMapStateRequests.size());

        ExecutingUpdateDirtyMapState = true;
        ExecuteTx(
            ctx,
            CreateTx<TUpdateDirtyMapState>(
                std::move(PendingUpdateDirtyMapStateRequests)));
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
