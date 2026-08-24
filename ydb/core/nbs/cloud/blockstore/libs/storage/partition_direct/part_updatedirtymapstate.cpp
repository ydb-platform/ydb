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
    db.StoreDirtyMapState(args.VChunkIndex, args.State);
}

void TPartitionActor::CompleteUpdateDirtyMapState(
    const TActorContext& ctx,
    TTxPartition::TUpdateDirtyMapState& args)
{
    Y_UNUSED(ctx);

    args.UpdateCompleted.SetValue();
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
