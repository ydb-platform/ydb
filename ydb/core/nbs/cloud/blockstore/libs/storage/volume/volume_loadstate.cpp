#include "volume_actor.h"
#include "volume_database.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::NBS_VOLUME

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

    YDB_LOG_INFO_CTX(ctx, "Loaded partition tablet id",
        {"partitionTabletId", PartitionTabletId});

    // Pipes open only after the id has been loaded.
    SignalTabletActive(ctx);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NStorage
