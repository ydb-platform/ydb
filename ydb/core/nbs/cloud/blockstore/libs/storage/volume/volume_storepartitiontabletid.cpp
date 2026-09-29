#include "volume_actor.h"
#include "volume_database.h"

namespace NYdb::NBS::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TVolumeActor::PrepareStorePartitionTabletId(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TStorePartitionTabletId& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TVolumeActor::ExecuteStorePartitionTabletId(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TStorePartitionTabletId& args)
{
    Y_UNUSED(ctx);

    TVolumeDatabase db(tx.DB);
    db.StorePartitionTabletId(args.PartitionTabletId);
}

void TVolumeActor::CompleteStorePartitionTabletId(
    const TActorContext& ctx,
    TTxVolume::TStorePartitionTabletId& args)
{
    if (PartitionTabletId != args.PartitionTabletId) {
        // The pipe and the requests it carries belong to the old partition.
        DropPartitionPipe(ctx, "partition tablet changed");
        PartitionTabletId = args.PartitionTabletId;
    }

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_VOLUME,
        "[%lu] Partition tablet %lu stored",
        TabletID(),
        PartitionTabletId);

    ReplyUpdateVolumeConfig(
        ctx,
        *args.RequestInfo,
        args.TxId,
        NKikimrBlockStore::OK);
}

}   // namespace NYdb::NBS::NStorage
