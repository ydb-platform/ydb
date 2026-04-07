#include "partition_direct_actor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_database.h>

#include <util/generic/fwd.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareStoreVolumeConfig(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TStoreVolumeConfig& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TPartitionActor::ExecuteStoreVolumeConfig(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TStoreVolumeConfig& args)
{
    Y_UNUSED(ctx);

    TPartitionDatabase db(tx.DB);
    db.StoreVolumeConfig(args.VolumeConfig);
}

void TPartitionActor::CompleteStoreVolumeConfig(
    const TActorContext& ctx,
    TTxPartition::TStoreVolumeConfig& args)
{
    VolumeConfig = args.VolumeConfig;
    Y_ABORT_UNLESS(VolumeConfig.PartitionsSize() == 1);

    AllocateDDiskBlockGroup(ctx);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
