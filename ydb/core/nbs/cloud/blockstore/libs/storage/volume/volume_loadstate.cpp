#include "volume_actor.h"
#include "volume_database.h"

namespace NYdb::NBS::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TVolumeActor::PrepareLoadState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TLoadState& args)
{
    Y_UNUSED(ctx);

    TVolumeDatabase db(tx.DB);
    return db.ReadPartitionTabletId(&args.PartitionTabletId);
}

void TVolumeActor::ExecuteLoadState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TLoadState& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TVolumeActor::CompleteLoadState(
    const TActorContext& ctx,
    TTxVolume::TLoadState& args)
{
    PartitionTabletId = args.PartitionTabletId;

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_VOLUME,
        "Loaded partition tablet id %lu",
        PartitionTabletId);

    // Pipes open only after the id has been loaded.
    SignalTabletActive(ctx);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NStorage
