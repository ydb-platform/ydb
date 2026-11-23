#include "part_database.h"
#include "partition_direct_actor.h"

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareSetVChunkTouched(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TSetVChunkTouched& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TPartitionActor::ExecuteSetVChunkTouched(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TSetVChunkTouched& args)
{
    Y_UNUSED(ctx);

    TPartitionDatabase db(tx.DB);
    for (const auto& chunk: args.Chunks) {
        db.StoreTouchedVChunkMask(chunk);
    }
}

void TPartitionActor::CompleteSetVChunkTouched(
    const TActorContext& ctx,
    TTxPartition::TSetVChunkTouched& args)
{
    Y_UNUSED(args);

    TouchedVChunks.OnSaveCompleted();

    if (TouchedVChunks.HasPendingChanges()) {
        ExecuteTx(ctx, CreateTx<TSetVChunkTouched>(TouchedVChunks.BeginSave()));
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
