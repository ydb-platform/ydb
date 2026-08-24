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

    Y_DEBUG_ABORT_UNLESS(ExecutingUpdateDirtyMapStatePromises.empty());
    ExecutingUpdateDirtyMapStatePromises.reserve(
        args.UpdateStateRequests.size());
    for (const auto& request: args.UpdateStateRequests) {
        ExecutingUpdateDirtyMapStatePromises.push_back(request.UpdateCompleted);
    }

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
        request.UpdateCompleted.TrySetValue(EPersistResult::Success);
    }
    ExecutingUpdateDirtyMapStatePromises.clear();
    ExecutingUpdateDirtyMapState = false;

    if (!PendingUpdateDirtyMapStateRequests.empty()) {
        LOG_INFO(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s Execute pending UpdateDirtyMapStateRequests %zu",
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
