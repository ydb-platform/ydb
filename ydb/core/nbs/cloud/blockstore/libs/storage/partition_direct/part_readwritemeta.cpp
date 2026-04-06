#include "partition_direct_actor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_database.h>

#include <util/generic/fwd.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareReadWriteMeta(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TReadWriteMeta& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "PartitionDirect schema initializing");

    TPartitionDatabase db(tx.DB);

    TMaybe<TString> meta;
    auto ok = db.ReadMeta(meta);

    if (!ok) {
        return false;
    }

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "PartitionDirect schema execution completed, meta: %s",
        meta.Defined() ? meta.GetRef().c_str() : "");

    return true;
}

void TPartitionActor::ExecuteReadWriteMeta(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TReadWriteMeta& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);

    TPartitionDatabase db(tx.DB);
    db.WriteMeta(args.Meta);

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "PartitionDirect schema execution completed");
}

void TPartitionActor::CompleteReadWriteMeta(
    const TActorContext& ctx,
    TTxPartition::TReadWriteMeta& args)
{
    Y_UNUSED(args);

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "PartitionDirect ReadWriteMeta completed");
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
