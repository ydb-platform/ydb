#include "part_database.h"
#include "partition_direct_actor.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::NBS_PARTITION

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
        YDB_LOG_INFO_CTX(ctx, "Execute pending UpdateVChunkStateRequests",
            {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
            {"#_PendingUpdateVChunkStateRequests.size", PendingUpdateVChunkStateRequests.size()});

        ExecutingUpdateVChunkState = true;
        ExecuteTx(
            ctx,
            CreateTx<TUpdateVChunkState>(
                std::move(PendingUpdateVChunkStateRequests)));
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
