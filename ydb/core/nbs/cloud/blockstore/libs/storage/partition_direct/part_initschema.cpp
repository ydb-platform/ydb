#include "partition_direct_actor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_database.h>

#include <util/generic/fwd.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::NBS_PARTITION

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareInitSchema(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TInitSchema& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    YDB_LOG_INFO_CTX(ctx, "PartitionDirect schema initializing");

    return true;
}

void TPartitionActor::ExecuteInitSchema(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TInitSchema& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);

    TPartitionDatabase db(tx.DB);
    db.InitSchema();

    YDB_LOG_INFO_CTX(ctx, "PartitionDirect schema execution completed");
}

void TPartitionActor::CompleteInitSchema(
    const TActorContext& ctx,
    TTxPartition::TInitSchema& args)
{
    Y_UNUSED(args);

    YDB_LOG_INFO_CTX(ctx, "PartitionDirect schema initialized");

    ExecuteTx(ctx, CreateTx<TLoadState>());
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
