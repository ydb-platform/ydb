#include "partition_direct_actor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_database.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareUpdateVChunkConfig(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TUpdateVChunkConfig& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);

    Y_DEBUG_ABORT_UNLESS(ExecutingUpdateVChunkConfigPromises.empty());
    ExecutingUpdateVChunkConfigPromises.reserve(
        args.UpdateConfigRequests.size());
    for (const auto& request: args.UpdateConfigRequests) {
        ExecutingUpdateVChunkConfigPromises.push_back(request.UpdateCompleted);
    }

    return true;
}

void TPartitionActor::ExecuteUpdateVChunkConfig(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TUpdateVChunkConfig& args)
{
    Y_UNUSED(ctx);

    TPartitionDatabase db(tx.DB);
    for (const auto& request: args.UpdateConfigRequests) {
        db.StoreVChunkConfig(request.VChunkConfig);
    }
}

void TPartitionActor::CompleteUpdateVChunkConfig(
    const TActorContext& ctx,
    TTxPartition::TUpdateVChunkConfig& args)
{
    for (auto& request: args.UpdateConfigRequests) {
        request.UpdateCompleted.TrySetValue(EPersistResult::Success);
    }
    ExecutingUpdateVChunkConfigPromises.clear();
    ExecutingUpdateVChunkConfig = false;

    if (!PendingUpdateVChunkConfigRequests.empty()) {
        LOG_INFO(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s Execute pending UpdateVChunkConfigRequests %zu",
            LogTitle.GetWithTime().c_str(),
            PendingUpdateVChunkConfigRequests.size());

        ExecutingUpdateVChunkConfig = true;
        ExecuteTx(
            ctx,
            CreateTx<TUpdateVChunkConfig>(
                std::move(PendingUpdateVChunkConfigRequests)));
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
