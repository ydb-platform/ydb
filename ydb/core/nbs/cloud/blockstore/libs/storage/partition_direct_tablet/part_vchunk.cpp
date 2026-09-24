#include "part_database.h"
#include "partition_direct_actor.h"

#include <util/system/datetime.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareCleanupDeletedDDisks(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TCleanupDeletedDDisks& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);

    args.RecordIds = DeletedDDiskStorage.MakeCleanupBatch(TInstant::Now());
    return true;
}

void TPartitionActor::ExecuteCleanupDeletedDDisks(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TCleanupDeletedDDisks& args)
{
    Y_UNUSED(ctx);

    TPartitionDatabase db(tx.DB);
    db.DeleteDeletedDDisk(args.RecordIds);
}

void TPartitionActor::CompleteCleanupDeletedDDisks(
    const TActorContext& ctx,
    TTxPartition::TCleanupDeletedDDisks& args)
{
    DeletedDDiskStorage.RemovePersisted(args.RecordIds);

    if (!args.RecordIds.empty()) {
        ExecuteTx(
            ctx,
            CreateTx<TCleanupDeletedDDisks>());
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
