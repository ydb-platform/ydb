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
    Y_UNUSED(args);

    return true;
}

void TPartitionActor::ExecuteUpdateVChunkConfig(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TUpdateVChunkConfig& args)
{
    Y_UNUSED(ctx);

    TPartitionDatabase db(tx.DB);
    db.StoreVChunkConfig(args.VChunkConfig);
}

void TPartitionActor::CompleteUpdateVChunkConfig(
    const TActorContext& ctx,
    TTxPartition::TUpdateVChunkConfig& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
