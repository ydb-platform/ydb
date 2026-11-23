#include "volume_actor.h"
#include "volume_database.h"

namespace NYdb::NBS::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TVolumeActor::PrepareInitSchema(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TInitSchema& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TVolumeActor::ExecuteInitSchema(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TInitSchema& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);

    TVolumeDatabase db(tx.DB);
    db.InitSchema();
}

void TVolumeActor::CompleteInitSchema(
    const TActorContext& ctx,
    TTxVolume::TInitSchema& args)
{
    Y_UNUSED(args);

    ExecuteTx(ctx, CreateTx<TLoadState>());
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NStorage
