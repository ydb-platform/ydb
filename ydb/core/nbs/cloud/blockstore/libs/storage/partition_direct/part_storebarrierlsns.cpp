#include "partition_direct_actor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_database.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareStoreBarrierLsns(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TStoreBarrierLsns& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TPartitionActor::ExecuteStoreBarrierLsns(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TStoreBarrierLsns& args)
{
    Y_UNUSED(ctx);

    TPartitionDatabase db(tx.DB);
    for (const auto& [dbgIndex, lsn]: args.PerDbgLsn) {
        db.StoreBarrierLsn(dbgIndex, lsn);
    }
}

void TPartitionActor::CompleteStoreBarrierLsns(
    const TActorContext& ctx,
    TTxPartition::TStoreBarrierLsns& args)
{
    OnBarrierLsnsPersisted(ctx, std::move(args.PerDbgLsn));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
