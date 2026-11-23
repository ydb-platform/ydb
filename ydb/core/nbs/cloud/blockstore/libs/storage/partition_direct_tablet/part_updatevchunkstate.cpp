#include "part_database.h"
#include "partition_direct_actor.h"

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareUpdateVChunkState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TUpdateVChunkState& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);

    Y_DEBUG_ABORT_UNLESS(ExecutingUpdateVChunkStatePromises.empty());
    ExecutingUpdateVChunkStatePromises.reserve(args.UpdateStateRequests.size());
    for (const auto& request: args.UpdateStateRequests) {
        ExecutingUpdateVChunkStatePromises.push_back(request.UpdateCompleted);
    }

    return true;
}

void TPartitionActor::ExecuteUpdateVChunkState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TUpdateVChunkState& args)
{
    Y_UNUSED(ctx);

    TPartitionDatabase db(tx.DB);
    for (const auto& request: args.UpdateStateRequests) {
        if (!request.VChunkConfig.Empty()) {
            db.StoreVChunkConfig(request.VChunkConfig);
        }
        db.StoreDirtyMapState(request.VChunkIndex, request.DirtyMapState);
    }
}

void TPartitionActor::CompleteUpdateVChunkState(
    const TActorContext& ctx,
    TTxPartition::TUpdateVChunkState& args)
{
    for (auto& request: args.UpdateStateRequests) {
        if (!request.VChunkConfig.Empty()) {
            VChunkConfigs[request.VChunkIndex] = request.VChunkConfig;
        }
        request.UpdateCompleted.TrySetValue(EPersistResult::Success);
    }
    ExecutingUpdateVChunkStatePromises.clear();
    ExecutingUpdateVChunkState = false;

    if (!PendingUpdateVChunkStateRequests.empty()) {
        LOG_INFO(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s Execute pending UpdateVChunkStateRequests %zu",
            LogTitle.GetWithTime().c_str(),
            PendingUpdateVChunkStateRequests.size());

        ExecutingUpdateVChunkState = true;
        ExecuteTx(
            ctx,
            CreateTx<TUpdateVChunkState>(
                std::move(PendingUpdateVChunkStateRequests)));
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
