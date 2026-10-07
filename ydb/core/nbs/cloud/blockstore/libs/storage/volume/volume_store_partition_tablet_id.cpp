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
    PartitionTabletId = args.PartitionTabletId;
    ForwardUpdateVolumeConfig(ctx, args.Record);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NStorage
