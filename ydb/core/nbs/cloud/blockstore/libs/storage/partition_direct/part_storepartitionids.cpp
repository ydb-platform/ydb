#include "partition_direct_actor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_database.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareStorePartitionIds(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TStorePartitionIds& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TPartitionActor::ExecuteStorePartitionIds(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TStorePartitionIds& args)
{
    Y_UNUSED(ctx);

    TPartitionDatabase db(tx.DB);
    db.StoreDirectBlockGroupsConnections(args.DirectBlockGroupsConnections);
}

void TPartitionActor::CompleteStorePartitionIds(
    const TActorContext& ctx,
    TTxPartition::TStorePartitionIds& args)
{
    Start(ctx, args.DirectBlockGroupsConnections);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
