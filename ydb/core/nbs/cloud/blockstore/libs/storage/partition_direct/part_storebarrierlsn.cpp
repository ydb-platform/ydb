#include "partition_direct_actor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_database.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareStoreBarrierLsn(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TStoreBarrierLsn& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TPartitionActor::ExecuteStoreBarrierLsn(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TStoreBarrierLsn& args)
{
    Y_UNUSED(ctx);

    TPartitionDatabase db(tx.DB);
    db.StoreBarrierLsn(args.DirectBlockGroupIndex, args.Lsn);
}

void TPartitionActor::CompleteStoreBarrierLsn(
    const TActorContext& ctx,
    TTxPartition::TStoreBarrierLsn& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
